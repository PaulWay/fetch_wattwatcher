# fetch_wattwatcher - A utility for downloading Wattwatcher data and
# uploading it to other sources.
# Copyright Paul Wayper 2020 and later.
# Licensed under version 3 of the GPL.

import argparse
from datetime import datetime, timedelta
import glob
from influxdb import InfluxDBClient
import json
from os import environ, path
import requests
import textwrap
import time
import yaml


####################
# Constants and defaults
config_location = 'settings.yaml'
valid_granularities = {'5m': 300, '15m': 900, '30m': 1800, 'hour': 3600}
valid_grain_list = sorted(
    valid_granularities.keys(), key=lambda g: valid_granularities[g]
)

####################
# Global variables
args = None
config = dict()
ww_headers = dict()
ww_base_url = 'https://api-v3.wattwatchers.com.au'
ww_file_pattern = "wattwatcher_{s}-{f}_{g}.json"
pv_headers = dict()
pv_base_url = 'https://pvoutput.org/service/r2/'
device_info = dict()
channel_info = dict()
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
    global device_info
    global channel_info
    if device_info and channel_info:
        # Work already done.
        return
    devices = ww_api_get('devices')
    assert isinstance(devices, list), f'got devices {devices}'
    # Get the device info for each device
    device_info = dict()
    channel_info = dict()
    for device in devices:
        device_info[device] = ww_api_get('devices/' + device)
        for channel_no, channel_data in enumerate(device_info[device]['channels']):
            channel_name = channel_data['label']
            # print(f"Got channel {channel_name} on device {device} port {channel_no} (from 0)")
            channel_info[channel_name] = channel_data
            channel_info[channel_name]['device'] = device
            channel_info[channel_name]['index'] = channel_no


def day_extents_timestamps(date=None):
    """
    Get the start and finish timestamps for a date, or today if no date
    specified.
    """
    if date is None:
        date = datetime.now()
    start = int(date.replace(hour=0, minute=0, second=0).timestamp())
    finit = int(date.replace(hour=23, minute=59, second=59).timestamp())
    return (start, finit)


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


def ww_get_usage_for_range(start_ts, finit_ts, granularity='5m'):
    """
    Get the power usage from all devices from the WattWatchers API for a
    given time range at the given granularity (which can be one of '5m',
    '15m', '30m', or 'hour'). The result is combined with metadata of the
    start and end time for this period.  The results are stored per channel
    across all devices and are rearranged into 'channel' -> 'timestamp' ->
    'value' - something like this:

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
    if args.verbose:
        print(f"Getting data from WattWatchers for {start_ts:.0f}..{finit_ts:.0f} per {granularity}")
    if granularity not in valid_granularities:
        pwrap(
            f"Warning: granularity should be one of {valid_grain_list}"
        )
        return None
    usage_data = {  # otherwise organised by timestamp to combine channels
        '_metadata_': {
            'start_ts': int(start_ts), 'finish_ts': int(finit_ts),
            'captured_ts': datetime.now().timestamp(),
            'granularity': granularity,
        }
    }
    min_found_ts = 0
    max_found_ts = 0
    for device, device_data in device_info.items():
        device_usage_data = ww_api_get(
            'long-energy/{device}?fromTs={fromTs}&toTs={toTs}&granularity={g}'.format(
                device=device,
                fromTs=int(start_ts),
                toTs=int(finit_ts),
                g=granularity
            )
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
    if args.verbose:
        print("... got usage data - keys:", usage_data.keys())
        print("... metadata:", usage_data['_metadata_'])
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
    if args.verbose:
        print(f"Storing usage data in '{filename}'")
    with open(path.join(data_dir, filename), 'w') as fh:
        json.dump(usage_data, fh)


def load_usage_data_from_data_dir(data_dir, start_ts, granularity='5m'):
    """
    Load data from a file with this start and granularity from the file
    system.  There must be a file with the given start time; if 'finish_ts'
    is left as None, the file with that start time and the largest finish
    time possible will be loaded.
    """
    file_glob = ww_file_pattern.format(
        s=int(start_ts), f='*', g=granularity
    )
    matching_files = glob.glob(path.join(data_dir, file_glob))
    if not matching_files:
        pwrap(
            f"Could not find any file named '{file_glob}' in '{data_dir}' "
            f"to match starting timestamp '{start_ts}'"
        )
        return {'_metadata_': {}}
    # Last one in sorted order will be with last matching end date
    filename = sorted(matching_files)[-1]
    if args.verbose:
        print(f"Trying to get data from {filename}...")
    with open(filename) as fh:
        return json.load(fh)


def get_usage_for_range(start_ts, finish_ts, granularity='5m'):
    """
    Get a range of data at the specified granularity, somehow.  If this range
    is covered by a file in our data directory, then use that.  If we have
    InfluxDB enabled, query that (TODO - not implemented yet).  Otherwise,
    fetch the range from the WattWatchers API.

    To start with, we're going to assume each range is exactly one day in
    length, and starts and finishes at the ends of that day - in other words,
    as returned by `day_extents_timestamps()` above.  In the future, we might
    support ranges that don't start or end on the day boundaries or span
    multiple days.

    We will, however, fetch extra data if we have a range that has been
    started but not yet completed - e.g. a day that was previously partly
    fetched.
    """
    if args.verbose:
        print(f"Get usage from {start_ts:.0f} to {finish_ts:.0f} every {granularity}")
    if not ('storage' in config and 'data_dir' in config['storage']):
        if args.verbose:
            print("... no storage, fetching whole range")
        return ww_get_usage_for_range(start_ts, finish_ts, granularity)

    usage_data = load_usage_data_from_data_dir(
        config['storage']['data_dir'], start_ts, granularity
    )
    meta = usage_data['_metadata_']
    if 'found_start_ts' not in meta and 'found_finish_ts' not in meta:
        # Nothing in this data segment - fetch and return it.
        if args.verbose:
            print("... No data in file...")
        return ww_get_usage_for_range(start_ts, finish_ts, granularity)
    # Maybe we need to fill later samples in this data?  Check that its
    # metadata found start and end ranges are what we expect.  We only
    # check the end of this range - if there aren't any samples before the
    # start of the day, maybe this is the first day?
    found_finish_ts = meta['found_finish_ts']
    rest_start_ts = found_finish_ts + valid_granularities[granularity]
    # range finish is 23:59:59 usually - round that down to the last
    # granularity range
    requested_finish_ts = finish_ts - valid_granularities[granularity] + 1
    if found_finish_ts >= requested_finish_ts:
        if args.verbose:
            print(f"... got data from file for range {meta['found_start_ts']} to {meta['found_finish_ts']}")
        return usage_data
    if args.verbose:
        print(
            f"... file data from {meta['found_start_ts']} to "
            f"{meta['found_finish_ts']}, fetching WattWatchers data from "
            f"{rest_start_ts} to {finish_ts} and merging"
        )
    rest_of_data = ww_get_usage_for_range(
        rest_start_ts, finish_ts, granularity
    )
    # If we didn't get any more data, maybe we're requesting it too soon?
    if 'found_start_ts' not in rest_of_data['_metadata_']:
        # We can return our fetched data now
        return usage_data
    # Check a couple of things here
    assert rest_start_ts == rest_of_data['_metadata_']['start_ts']
    # Merge these together:
    meta['found_finish_ts'] = rest_of_data['_metadata_']['found_finish_ts']
    meta['captured_ts'] = rest_of_data['_metadata_']['captured_ts']
    for channel, channel_data in usage_data.items():
        if channel != '_metadata_':
            # Channel metadata should be updated
            channel_data.update(rest_of_data[channel])
    print("... merged metadata:", usage_data['_metadata_'])
    print("... house keys:", usage_data['House'].keys())
    return usage_data


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
        for timestamp, time_data in channel_data.items()
        if channel != '_metadata_' and timestamp != '_metadata_'
    ]
    print("Data points mangled into:", data_points)
    set_up_influxdb(config['influxdb'])
    global influxdb
    influxdb.write_points(data_points, time_precision='s')


def pv_channel_data_config():
    """
    Get the PVOutput generation and consumption channel names from the
    configuration file.
    """
    if 'pvoutput' not in config:
        return
    if 'generation_channel' not in config['pvoutput']:
        return
    pv_gen_channel = config['pvoutput']['generation_channel']
    pv_con_channel = config['pvoutput'].get('consumption_channel', None)
    return (pv_gen_channel, pv_con_channel)


def get_pv_channel_data(pv_gen_channel, pv_con_channel):
    """
    Get the WattWatchers devices and find the PVOutput generation and
    consumption channels in them, returning the device and channel numbers
    for each.
    """
    get_devices()
    if pv_gen_channel not in channel_info:
        pwrap(
            f"Warning: generation channel '{pv_gen_channel}' specified in "
            f"config but not found in channel data from WattWatchers"
        )
        return
    pv_gen_device = channel_info[pv_gen_channel]['device']
    pv_gen_chan_no = channel_info[pv_gen_channel]['index']
    print(f"Channel '{pv_gen_channel}' is device {pv_gen_device} port {pv_gen_chan_no}")
    if pv_con_channel:
        if pv_con_channel not in channel_info:
            pwrap(
                f"Warning: consumption channel '{pv_con_channel}' specified in "
                f"config but not found in channel data from WattWatchers ("
                f"{', '.join(sorted(channel_info.keys()))}) - ignoring"
            )
            pv_con_channel = None
        else:
            pv_con_device = channel_info[pv_con_channel]['device']
            pv_con_chan_no = channel_info[pv_con_channel]['index']
            if pv_con_device != pv_gen_device:
                pwrap(
                    f"Warning: at the moment we don't support the consumption "
                    f"device for channel '{pv_con_channel}' (which is "
                    f"'{pv_con_device}') being different from the generation "
                    f"device ('{pv_gen_device}') for channel '{pv_gen_channel}' "
                    f"- ignoring"
                )
                pv_con_channel = None
            else:
                print(f"Channel {pv_con_channel} is device: {pv_con_device} port {pv_con_chan_no}")
        return (pv_gen_device, pv_gen_chan_no, pv_con_device, pv_con_chan_no)
    else:
        return (pv_gen_device, pv_gen_chan_no, None, None)


def store_usage_to_pvoutput(usage_data):
    """
    Find a particular channel of usage data and store that in PVOutput.
    """
    pv_gen_channel, pv_con_channel = pv_channel_data_config()
    if not pv_gen_channel:
        return
    (
        pv_gen_device, pv_gen_chan_no, pv_con_device, pv_con_chan_no
    ) = get_pv_channel_data(pv_gen_channel, pv_con_channel)
    pv_headers = {
        'X-Pvoutput-Apikey': config['pvoutput']['api_key'],
        'X-Pvoutput-SystemId': str(config['pvoutput']['system_id']),
    }
    sample_format = '{date},{time},,{pgen},,{pcon},,{mvol}'
    if args.verbose:
        print(
            f"Sending usage data from {usage_data['_metadata_']['found_start_ts']} "
            f"to {usage_data['_metadata_']['found_start_ts']}..."
        )

    def batch_usage_data(batch_size=30):
        """
        Step through the time periods in this batch and output a batch output
        string for the generation and consumption channels, assuming complete
        contiguous samples from start_ts to (finish_ts mod granularity).
        """
        sample_outputs = []
        sample_dt = datetime.fromtimestamp(usage_data['_metadata_']['found_start_ts'])
        finish_dt = datetime.fromtimestamp(usage_data['_metadata_']['found_finish_ts'])
        gran_tdel = timedelta(minutes=int(usage_data['_metadata_']['granularity'][:-1]))
        while sample_dt <= finish_dt:
            gen_sample = usage_data[pv_gen_channel][sample_dt.timestamp()]
            gen_joules = gen_sample['eRealPositive']
            gen_power = int(gen_joules / gran_tdel.seconds + 0.5)
            gen_volts = '{:.1f}'.format((gen_sample['vRMSMin'] + gen_sample['vRMSMax']) / 2)
            if pv_con_channel:
                con_joules = usage_data[pv_con_channel][sample_dt.timestamp()]['eRealPositive']
                con_power = int(con_joules / gran_tdel.seconds + 0.5)
            else:
                con_energy = ''
                con_power = ''
            sample_outputs.append(sample_format.format(
                date=sample_dt.strftime('%Y%m%d'),
                time=sample_dt.strftime('%H:%M'),
                pgen=gen_power, pcon=con_power, mvol=gen_volts,
            ))
            if len(sample_outputs) == batch_size:
                yield ';'.join(sample_outputs)
                sample_outputs = []
            sample_dt += gran_tdel
        yield ';'.join(sample_outputs)

    # At this time post all the timestamps of data we've got
    for delimited in batch_usage_data():
        response = requests.post(
            pv_base_url + 'addbatchstatus.jsp',
            data={'data': delimited},
            headers=pv_headers
        )
        if response.status_code == 200 and args.verbose:
            resp_parts = response.content.decode().split(';')
            s_date, s_time, _ = resp_parts[0].split(',')
            f_date, f_time, _ = resp_parts[-1].split(',')
            OKs = ','.join(
                'OK' if p.split(',')[2] == '1' else 'bad'
                for p in resp_parts
            )
            print(
                f"Date {s_date} push from {s_time} to {f_time}: {OKs}"
            )


def fetch_today():
    """
    Just fetch today's data from the WattWatcher site.
    """
    if args.verbose:
        print("Fetching all data for today")
    get_devices()
    start_of_today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    last_second_today = start_of_today + timedelta(days=1, seconds=-1)
    usage_data = get_usage_for_range(
        start_of_today.timestamp(), last_second_today.timestamp()
    )
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
    store_usage_to_pvoutput(usage_data)


def pvoutput_for_date():
    """
    Fetch all data for a specific day and put it in PVOutput
    """
    if not args.date:
        print("--date option required for `pvoutput-for-date` mode.")
        exit(1)
    if args.verbose:
        print("Fetching all data for", args.date)
    get_devices()
    start_of_day = args.date
    last_second_in_day = args.date + timedelta(days=1, seconds=-1)
    usage_data = get_usage_for_range(
        start_of_day.timestamp(), last_second_in_day.timestamp()
    )
    if 'storage' in config and 'data_dir' in config['storage']:
        store_usage_data_in_data_dir(usage_data, config['storage']['data_dir'])
    store_usage_to_pvoutput(usage_data)


def push_influxdb():
    usage_data = fetch_today()
    if all(
        'influxdb' in config,
        config['influxdb'].get('enabled', 'true') == 'true'
    ):
        store_usage_data_in_influxdb(usage_data)


def update_current():
    (
        pv_gen_channel, pv_gen_device, pv_gen_chan_no,
        pv_con_channel, pv_con_device, pv_con_chan_no
    ) = get_pv_channel_data()
    if not pv_gen_channel:
        return
    pv_headers = {
        'X-Pvoutput-Apikey': config['pvoutput']['api_key'],
        'X-Pvoutput-SystemId': str(config['pvoutput']['system_id']),
    }

    while True:
        # Current time, truncate to nearest 5 minutes
        now = datetime.now()
        print(f"Now: {now} timestamp {now.timestamp()}")
        end_of_period = now.replace(
            minute=int(now.minute / 5) * 5, second=0, microsecond=0
        )
        start_of_period = end_of_period - timedelta(minutes=5)
        print(
            f"Fetching data from {start_of_period} ({start_of_period.timestamp()}) "
            f"to {end_of_period} ({end_of_period.timestamp()})"
        )
        device_usage_data = ww_api_get(
            'long-energy/{device}?fromTs={fromTs}&toTs={toTs}'.format(
                device=pv_gen_device,
                fromTs=int(start_of_period.timestamp()),
                toTs=int(end_of_period.timestamp()),
            )
        )
        assert isinstance(device_usage_data, list)
        datapoint = device_usage_data[0]
        print(f"From WattWatchers got timestamp {datapoint['timestamp']} <=> {start_of_period.timestamp()}")
        print(
            f"... generation: {datapoint['eRealPositive'][pv_gen_chan_no]}J = "
            f"{datapoint['eRealPositive'][pv_gen_chan_no] / datapoint['duration']:.2f}W, "
            f"min {datapoint['vRMSMin'][pv_gen_chan_no]}V max {datapoint['vRMSMax'][pv_gen_chan_no]}V"
        )
        if pv_con_channel:
            print(
                f"... consumption: {datapoint['eRealPositive'][pv_con_chan_no]:.2f}J = "
                f"{datapoint['eRealPositive'][pv_gen_chan_no] / datapoint['duration']}W"
            )


        # NOTE: PVOutput doesn't let energy readings (WH) go below any previous
        # reading.  So we have to push watts.
        # A Joule is a watt-second.  Divide by seconds in period to get watts.
        parameters = {
            'v2': int(datapoint['eRealPositive'][pv_gen_chan_no] / datapoint['duration']),
            'd': start_of_period.strftime('%Y%m%d'), 't': start_of_period.strftime('%H:%M'),
            'v6': int((
                datapoint['vRMSMin'][pv_gen_chan_no] + datapoint['vRMSMax'][pv_gen_chan_no]
            ) / 2),
        }
        if pv_con_channel:
            parameters['v4'] = int(datapoint['eRealPositive'][pv_con_chan_no] / datapoint['duration'])
        print("Parameters:", parameters)
        response = requests.post(
            pv_base_url + 'addstatus.jsp',
            data=parameters,
            headers=pv_headers
        )
        print(response.content.decode())
        assert response.status_code == 200
        # We want to sleep until a certain fudge factor past the five minute
        # boundary, to allow for data to be transmitted to WattWatchers and
        # calculated if necessary, as well as possible clock misalignment.
        # Let's say 30 seconds past the end of the most recent period.
        delay = (end_of_period.timestamp() + 330) - time.time()
        print(f"Sleeping {delay:.2f} seconds until {end_of_period + timedelta(seconds=330)}")
        time.sleep(delay)


####################
# Modes and function to execute them
modes = {
    'fetch': fetch_today,
    'fetch-all': fetch_all_data,
    'push-pvoutput': push_pvoutput,
    'push-influxdb': push_influxdb,
    'pvoutput-for-date': pvoutput_for_date,
    'update-current': update_current,
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
        '--date', '-d', action='store', default=None,
        help='The date to fetch (for date-specific operations)'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true', default=False,
        help="Tell the user what is being done",
    )
    parser.add_argument(
        'mode', default='fetch', choices=modes,
        help='What to do'
    )
    args = parser.parse_args()
    # Check arguments if necessary
    if args.date:
        try:
            args.date = datetime.strptime(args.date, '%Y-%m-%d')
        except:
            print(f"Error: cannot parse '{args.date}' in YYYY-MM-DD format")
            exit(1)


####################
# Main code
if __name__ == '__main__':
    parse_arguments()
    read_config()
    # Call mode function
    modes[args.mode]()
