# fetch_wattwatcher - A utility for downloading Wattwatcher data and
# uploading it to other sources.
# Copyright Paul Wayper 2020 and later.
# Licensed under version 3 of the GPL.

import argparse
from datetime import datetime
import glob
from influxdb import InfluxDBClient
import json
from os import environ, path
import requests
import textwrap
import yaml


####################
# Constants and defaults
config_location = 'settings.yaml'

####################
# Global variables
args = None
config = dict()
ww_headers = dict()
ww_base_url = 'https://api-v3.wattwatchers.com.au'
ww_file_pattern = "wattwatcher_{s}-{f}_{g}.json"
pv_headers = dict()
pv_status_url = 'https://pvoutput.org/service/r2/addstatus.jsp'
device_info = dict()
influx_client = None
usage_storage = dict()


def pwrap(*args):
    print(textwrap.fill(*args))


def read_config():
    """
    Read configuration file
    """
    global config
    with open(args.config_file) as fh:
        config = yaml.safe_load(fh)

    # Sanity checks:
    if 'wattwatchers' not in config:
        print("Error: 'wattwatchers' section not in config file.")
        exit(1)
    if not isinstance(config['wattwatchers'], dict):
        print(
            "Error: 'wattwatchers' section in config file is not a dictionary."
        )
        exit(1)
    if 'pvoutput' not in config:
        print("Error: 'pvoutput' section not in config file.")
        exit(1)
    if not isinstance(config['pvoutput'], dict):
        print("Error: 'pvoutput' section in config file is not a dictionary.")
        exit(1)

    global ww_headers, ww_base_url
    ww_api_key = environ.get(
        'WATTWATCHER_API_KEY', config['wattwatchers']['api_key']
    )
    if 'base_url' in config['wattwatchers']:
        ww_base_url = config['wattwatchers']['base_url']
    if not (ww_api_key and ww_api_key.startswith('key_')):
        pwrap(
            "Error: WattWatchers API key not found or does not start "
            "with 'key_'.  Set via config file under wattwatchers.api_key "
            "or use environment variable WATTWATCHERS_API_KEY."
        )
        exit(1)
    ww_headers = {
        'Authorization': 'Bearer ' + ww_api_key
    }

    pv_api_key = environ.get('PVOUTPUT_API_KEY', config['pvoutput']['api_key'])
    if not (pv_api_key and len(pv_api_key) == 40):
        pwrap(
            "Error: PVOutput API key not found or not correct length. "
            "Set via config file under pvoutput.api_key "
            "or use environment variable PVOUTPUT_API_KEY."
        )
        exit(1)
    if 'pvoutput' in config and 'system_id' not in config['pvoutput']:
        print(
            "Error: PVOutput System ID not set in 'system_id' key in pvoutput "
            "config"
        )
        exit(1)


def set_up_influxdb(influx_config):
    """
    Start the connection to the InfluxDB server, if necessary.
    """
    global influx_client
    if influx_client is not None:
        return
    influx_host = influx_config.get('host', 'localhost')
    influx_port = int(influx_config.get('port', '8086'))
    influx_user = influx_config.get('username', 'influxdb')
    influx_pass = influx_config.get('password', 'influxdb')
    influx_database = influx_config.get('database', 'energy_data')
    influx_client = InfluxDBClient(
        host=influx_host, port=influx_port,
        username=influx_user, password=influx_pass,
    )
    if influx_database not in (
        db['name'] for db in influx_client.get_list_database()
    ):
        influx_client.create_database(influx_database)
    influx_client.switch_database(influx_database)


def ww_api_get(endpoint, parameters=None):
    """
    Get an endpoint from the Wattwatchers API, optionally with a
    dictionary of parameters.
    """
    response = requests.get(
        f"{ww_base_url}/{endpoint}",
        data=parameters,
        headers=ww_headers
    )
    if response.status_code != 200:
        pwrap(
            f"Error: GET {endpoint} returned status {response.status_code} - "
            f"{response.data}"
        )
        exit(1)
    return response.json()


def get_devices():
    """
    Get the list of devices and get the information on each device
    """
    devices = ww_api_get('devices')
    assert isinstance(devices, list), f'got devices {devices}'
    # Get the device info for each device
    global device_info
    device_info = {}
    for device in devices:
        device_info[device] = ww_api_get('devices/' + device)
        print(f"Device {device} channels: {device_info[device]['channels']}")


def device_channel_data(device, channel):
    """
    Look up the channel data from a device by channel number or name
    """
    if 'channels' not in device_info[device]:
        print(f"Error: no 'channels' section in device info for {device}")
        return
    if isinstance(channel, int):
        # Look for channel by number, starting from channel 1
        if channel - 1 > len(device_info[device]['channels']):
            pwrap(
                f"Warning: channel {channel} not found in channels of "
                f"{device}: {len(device_info[device]['channels'])} known"
            )
            return {}
        channel_data = device_info[device]['channels'][channel + 1]
    elif isinstance(channel, str):
        # Look for channel by name:
        for channel_data in device_info[device]['channels']:
            if channel_data['label'] == channel:
                break
        else:
            pwrap(
                f"Warning: could not find channel labelled '{channel}' "
                f"in device {device} - we know about ",
                ', '.join(
                    chan['label'] for chan in device_info[device]['channels']
                )
            )
            return {}
    return channel_data


def get_device_daily_usage(device, resolution='5m'):
    """
    Get the power usage from this device for today, at the given
    resolution (which can be one of '5m', '15m', '30m', or 'hour').
    """
    if device not in device_info:
        pwrap(
            f"Warning: device {device} not found in data from "
            f"WattWatchers - we know about {', '.join(device_info.keys())}"
        )
        return None
    valid_resolutions = ('5m', '15m', '30m', 'hour')
    if resolution not in valid_resolutions:
        pwrap(
            f"Warning: resolution should be one of {valid_resolutions}"
        )
        return None
    now = datetime.now()
    # Probably a good point here to fetch from InfluxDB which samples
    # we've already collected, and update our start timestamp.
    start = int(now.replace(hour=0, minute=0, second=0).timestamp())
    finit = int(now.replace(hour=23, minute=59, second=59).timestamp())
    usage_data = ww_api_get(
        'long-energy/' + device,
        {'fromTs': start, 'toTs': finit, 'granularity': resolution}
    )
    # Data comes back
    print("Got usage data", usage_data, "for device", device)
    return usage_data


def sample_to_fields(sample, pos):
    """
    Each sample in the long data is in the form:
    {
        "timestamp": 1550408400,
        "duration": 900,
        "eReal": [
            3639405, 336898, -560, 37, -14, -25
        ],
        ...
    }
    When we're compiling the data for one channel, we want to get that
    vertical 'slice' of samples from all fields that are lists of
    samples.  So we take a sample, and the position to slice through,
    and return (e.g. for position 1):
    {
        'eReal': 336898, 'eRealNegative': 0, 'eRealPositive': 336898,
        'eReactive': 140216, ...
    }
    Only fields that have a list value are captured in this slice.
    """
    return {
        key: val[pos]
        for key, val in sample.items()
        if isinstance(val, list)
    }


def get_daily_usage(resolution='5m'):
    """
    Get the power usage from all devices for today, at the given
    resolution (which can be one of '5m', '15m', '30m', or 'hour').
    The result is combined with metadata of the start and end time for this
    day.  The results are stored per channel across all devices and are
    rearranged into 'channel' -> 'timestamp' -> 'value' - something like this:

    {
      "House": {
        "1584492900": {
          "eReal": 66564,
          "eRealNegative": 0,
          "eRealPositive": 66564,
          "eReactive": -1215,
          "eReactiveNegative": 4800,
          "eReactivePositive": 3585,
          "vRMSMin": 244.1,
          "vRMSMax": 244.9,
          "iRMSMin": 0.761,
          "iRMSMax": 1.074
        },
        "1584493200": {
          "eReal": 67548,
          "eRealNegative": 0,
          "eRealPositive": 67548,
          "eReactive": 4068,
          "eReactiveNegative": 0,
          "eReactivePositive": 4068,
          "vRMSMin": 244,
          "vRMSMax": 244.7,
          "iRMSMin": 0.798,
          "iRMSMax": 0.97
        },
      ...
    }

    Data from the WattWatchers devices are stored in Joules.  Since one
    kilowatt-hour is 3600000 joules, this provides better precision in
    integer format.

    """
    valid_resolutions = ('5m', '15m', '30m', 'hour')
    if resolution not in valid_resolutions:
        pwrap(
            f"Warning: resolution should be one of {valid_resolutions}"
        )
        return None
    now = datetime.now()
    # Probably a good point here to fetch from InfluxDB which samples
    # we've already collected, and update our start timestamp.
    start = int(now.replace(hour=0, minute=0, second=0).timestamp())
    finit = int(now.replace(hour=23, minute=59, second=59).timestamp())
    usage_data = {  # otherwise organised by timestamp to combine channels
        '_metadata_': {
            'start_ts': start, 'finish_ts': finit,
            'captured_ts': now.timestamp(), 'granularity': resolution,
        }
    }
    min_found_ts = 0
    max_found_ts = 0
    for device, device_data in device_info.items():
        device_usage_data = ww_api_get(
            'long-energy/' + device,
            {'fromTs': start, 'toTs': finit, 'granularity': resolution}
        )
        # Get the minimum and maximum actual time stamps from the first
        # and last samples of the first channel of data we find
        if min_found_ts == 0:
            min_found_ts = int(device_usage_data[0]['timestamp'])
        if max_found_ts == 0:
            max_found_ts = int(device_usage_data[-1]['timestamp'])
        # Data comes back in lists, one item per channel; we want to
        # rearrange that into usage[channel][timestamp][field] - since
        # the channel shouldn't already be in there.
        for fnum, channel_data in enumerate(device_data['channels']):
            channel = channel_data['label']
            if channel in device_usage_data:
                pwrap(
                    f"Warning: channel named 'channel' appears twice in "
                    f"device data - rename one of them.  Skipping the one "
                    f"on device '{device}'."
                )
                continue
            usage_data[channel] = {
                sample['timestamp']: sample_to_fields(sample, fnum)
                for sample in device_usage_data
            }
            # Store device name in channel metadata
            usage_data[channel]['_metadata_'] = {
                'device': device
            }
    if min_found_ts > 0:
        usage_data['_metadata_']['found_start_ts'] = min_found_ts
    if max_found_ts > 0:
        usage_data['_metadata_']['found_finish_ts'] = max_found_ts
    print("Got usage data - keys:", usage_data.keys())
    return usage_data


def store_usage_data_in_data_dir(usage_data, data_dir):
    """
    Store the usage data we've collected in our data directory.

    Each file is named 'wattwatcher_{start_ts}-{finish_ts}-{granularity}.json
    - e.g. wattwatcher_1550408400_1550494800_5m.json
    """
    meta = usage_data['_metadata_']
    # Here we should NOT trust the metadata of the start and finish timestamps
    # that were *requested*, we should trust what we *actually got*.
    if 'found_start_ts' not in meta or 'found_finish_ts' not in meta:
        pwrap(
            f"Warning: cannot save usage data - while it was requested from "
            f"{meta['start_ts']}-{meta['finish_ts']}, there were no actual "
            f"samples found in the returned data.  Will not save this file."
        )
        return
    filename = ww_file_pattern.format(
        s=meta['found_start_ts'], f=meta['found_finish_ts'],
        g=meta['granularity']
    )
    with open(path.join(data_dir, filename), 'w') as fh:
        json.dump(usage_data, fh)


def load_usage_data_from_data_dir(
    data_dir, start_ts, finish_ts=None, granularity='5m'
):
    """
    Load data from a file with these dates and granularity from the file
    system.  There must be a file with the given start time; if 'finish_ts'
    is left as None, the file with that start time and the largest finish
    time possible will be loaded.
    """
    if finish_ts is None:
        file_glob = ww_file_pattern.format(
            s=start_ts, f='*', g=granularity
        )
        matching_files = glob.glob(path.join(data_dir, file_glob))
        if not matching_files:
            pwrap(
                f"Could not find any file named '{file_glob}' in '{data_dir}' "
                f"to match starting timestamp '{start_ts}'"
            )
            return {}
        # Last one in sorted order will be with last matching end date
        filename = sorted(matching_files)[-1]
    else:
        filename = ww_file_pattern.format(
            s=start_ts, f=finish_ts, g=granularity
        )
    with open(path.join(data_dir, filename)) as fh:
        return json.load(fh)


def store_usage_data_in_influxdb(usage_data):
    """
    Store the usage data in InfluxDB.
    """
    assert 'influxdb' in config, 'Config does not contain an influxdb section'
    # Mostly just arranging the tags and avoiding outputting metadata
    data_points = [
        {
            'measurement': 'energy_data',
            'tags': {
                'device': channel['_metadata_']['device'],
                'channel': channel,
            },
            'fields': time_data,
            'time': timestamp
        }
        for channel, channel_data in usage_data.items()
        for timestamp, time_data in channel_data
        if channel != '_metadata_' and timestamp != '_metadata_'
    ]
    print("Data points mangled into:", data_points)
    set_up_influxdb(config['influxdb'])
    global influxdb
    influxdb.write_points(data_points, time_precision='s')


def store_channel_to_pvoutput(usage_data, pv_channel):
    """
    Find a particular channel of usage data and store that in PVOutput.
    """
    pv_headers = {
        'X-Pvoutput-Apikey': config['pvoutput']['api_key'],
        'X-Pvoutput-SystemId': config['pvoutput']['system_id'],
    }
    if pv_channel not in usage_data:
        pwrap(
            f"Warning: channel '{pv_channel}' specified in config but not "
            f"found in usage data"
        )
        return
    parameters = {
        'd': 'date', 't': 'time',
        'v1': 'watt hours generated', 'v3': 'watt hours consumed',
        'v6': 'mean volts RMS',
    }
    # At this time post all the timestamps of data we've got
    response = requests.post(
        pv_status_url,
        data=parameters,
        headers=pv_headers
    )
    assert response.status_code == 200


def fetch_today():
    """
    Just fetch today's data from the WattWatcher site.
    """
    get_devices()
    usage_data = get_daily_usage()
    if 'storage' in config and 'data_dir' in config['storage']:
        store_usage_data_in_data_dir(usage_data, config['storage']['data_dir'])
    return usage_data


def fetch_all_data():
    """
    Fetch all data, from the first day we can to now.
    """
    get_devices()
    #
    # if 'storage' in config and 'data_dir' in config['storage']:
    #     store_usage_data_in_data_dir(
    #         usage_data, config['storage']['data_dir']
    #     )


def push_pvoutput():
    usage_data = fetch_today()
    if all(
        'pvoutput' in config,
        'wattwatcher_channel' in config['pvoutput'],
        config['pvoutput'].get('enabled', 'true') == 'true'
    ):
        store_channel_to_pvoutput(
            usage_data, config['pvoutput']['wattwatcher_channel']
        )


def push_influxdb():
    usage_data = fetch_today()
    if all(
        'influxdb' in config,
        config['influxdb'].get('enabled', 'true') == 'true'
    ):
        store_usage_data_in_influxdb(usage_data)


####################
# Modes and function to execute them
modes = {
    'fetch': fetch_today,
    'fetch-all': fetch_all_data,
    'push-pvoutput': push_pvoutput,
    'push-influxdb': push_influxdb,
}


def parse_arguments():
    global args
    parser = argparse.ArgumentParser(
        description='Fetch, manipulate and store WattWatcher Auditor data'
    )
    parser.add_argument(
        '--config-file', '-f', action='store',
        default=config_location,
        help='The YAML configuration file'
    )
    parser.add_argument(
        'mode', default='fetch', choices=modes,
        help='What to do'
    )
    args = parser.parse_args()


####################
# Main code
if __name__ == '__main__':
    parse_arguments()
    read_config()
    # Call mode function
    modes[args.mode]()
