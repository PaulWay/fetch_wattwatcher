# fetch_wattwatcher - A utility for downloading Wattwatcher data and
# uploading it to other sources.
# Copyright Paul Wayper 2020 and later.
# Licensed under version 3 of the GPL.

import argparse
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


def ww_api_get(endpoint, parameters=None):
    # Get an endpoint from the Wattwatchers API, optionally with a
    # dictionary of parameters.
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
    # Get list of devices
    devices = ww_api_get('devices')
    assert isinstance(devices, list), f'got devices {devices}'
    # Get the device info for each device
    device_info = {}
    for device in devices:
        device_info[device] = ww_api_get('devices/' + device)
        print(f"Device {device} channels: {device_info[device]['channels']}")


if __name__ == '__main__':
    parse_arguments()
    read_config()
    get_devices()
