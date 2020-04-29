# fetch_wattwatcher - A utility for downloading Wattwatcher data and
# uploading it to other sources.
# Copyright Paul Wayper 2020 and later.
# Licensed under version 3 of the GPL.

import argparse
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from os import environ
import requests
import textwrap
import yaml


####################
# Constants and defaults
config_location='/home/paulway/Coding/fetch_wattwatcher/settings.yaml'

####################
# Global variables
args = None
config = dict()
ww_headers = dict()
ww_base_url = 'https://api-v3.wattwatchers.com.au'
pv_headers = dict()
device_info = dict()
influx_client = None


def parse_arguments():
    global args
    parser = argparse.ArgumentParser(
        description='A Canberra Woodturner sales processor'
    )
    parser.add_argument(
        '--config-file', '-f', action='store',
        default=config_location,
        help='The YAML configuration file'
    )
    args = parser.parse_args()


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
        print("Error: 'wattwatchers' section in config file is not a dictionary.")
        exit(1)
    if 'pvoutput' not in config:
        print("Error: 'pvoutput' section not in config file.")
        exit(1)
    if not isinstance(config['pvoutput'], dict):
        print("Error: 'pvoutput' section in config file is not a dictionary.")
        exit(1)

    global ww_headers, ww_base_url
    ww_api_key = environ.get('WATTWATCHER_API_KEY', config['wattwatchers']['api_key'])
    if 'base_url' in config['wattwatchers']:
        ww_base_url = config['wattwatchers']['base_url']
    if not (ww_api_key and ww_api_key.startswith('key_')):
        print(textwrap.fill(
            "Error: WattWatchers API key not found or does not start "
            "with 'key_'.  Set via config file under wattwatchers.api_key "
            "or use environment variable WATTWATCHERS_API_KEY."
        ))
        exit(1)
    ww_headers = {
        'Authorization': 'Bearer ' + ww_api_key
    }
    
    pv_api_key = environ.get('PVOUTPUT_API_KEY', config['pvoutput']['api_key'])
    if not (pv_api_key and len(pv_api_key) == 40):
        print(textwrap.fill(
            "Error: PVOutput API key not found or not correct length. "
            "Set via config file under pvoutput.api_key "
            "or use environment variable PVOUTPUT_API_KEY."
        ))
        exit(1)
    if 'system_id' not in config['pvoutput']:
        print("Error: PVOutput System ID not set in 'system_id' key in pvoutput config")
        exit(1)

    if 'influxdb' in config:
        influx_host = config['influxdb'].get('host', 'localhost')
        influx_port = int(config['influxdb'].get('port', '8086'))
        influx_user = config['influxdb'].get('username', 'influxdb')
        influx_pass = config['influxdb'].get('password', 'influxdb')
        influx_database = config['influxdb'].get('database', 'energy_data')
        global influx_client
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
        print(f"Error: GET {endpoint} returned status {response.status_code} - {response.data}")
        exit(1)
    # print(f"GET {endpoint} returned {response.status_code}: {response.json()}")
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
            print(textwrap.fill(
                f"Warning: channel {channel} not found in channels of "
                f"{device}: {len(device_info[device]['channels'])} known"
            ))
            return {}
        channel_data = device_info[device]['channels'][channel + 1]
    elif isinstance(channel, str):
        # Look for channel by name:
        for channel_data in device_info[device]['channels']:
            if channel_data['label'] == channel:
                break
        else:
            print(textwrap.fill(
                f"Warning: could not find channel labelled '{channel}' "
                f"in device {device} - we know about "
                f"{', '.join(chan['label'] for chan in device_info[device]['channels'])}"
            ))
            return {}
    return channel_data


def get_device_daily_usage(device, resolution='5m'):
    """
    Get the power usage from this device for today, at the given
    resolution (which can be one of '5m', '15m', '30m', or 'hour').
    """
    if device not in device_info:
        print(textwrap.fill(
            f"Warning: device {device} not found in data from "
            f"WattWatchers - we know about {', '.join(device_info.keys())}"
        ))
        return None
    valid_resolutions = ('5m', '15m', '30m', 'hour')
    if resolution not in valid_resolutions:
        print(textwrap.fill(
            f"Warning: resolution should be one of {valid_resolutions}"
        ))
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
    print("Got usage data", usage_data, "for device", device)
    return usage_data

def store_usage_data_in_influxdb(device, usage_data)
    channel_names = [
        channel['label']
        for channel in device_info[device]['channels']
    ]
    # We want to submit each channel as a tag (indexed), and its fields
    # are the values reported.  So we have to reorganise this table.
    def sample_to_fields(sample, pos):
        return {
            key: val[pos]
            for key, val in sample.items()
            if isinstance(val, list)
        }
    data_points = [
        {
            'measurement': 'energy_data',
            'tags': {
                'device': device,
                'channel': channel,
            },
            'fields': sample_to_fields(sample, pos),
            'time': sample['timestamp']
        }
        for pos, channel in enumerate(channel_names)
        for sample in usage_data
    ]
    print("Data points mangled into:", data_points)
    influxdb.write_points(data_points, time_precision='s')

if __name__ == '__main__':
    parse_arguments()
    read_config()
    get_devices()
    if 'pvoutput' in config and 'wattwatcher_device' in config['pvoutput'] and 'wattwatcher_channel' in config['pvoutput']:
        usage_data = get_device_daily_usage(config['pvoutput']['wattwatcher_device'])
        if 'influxdb' in config:
            store_usage_data_in_influxdb(config['pvoutput']['wattwatcher_device'], usage_data)
