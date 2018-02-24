#!/usr/bin/env python
#
# Read CSV data and send to the REST gateway.
#
# Written by: claudio.fahey@dell.com
#

from __future__ import division
from __future__ import print_function
from time import sleep, time
import requests
from optparse import OptionParser
from datetime import datetime

def playback_recorded_file_generator(filename):
    with open(filename, 'r') as f:
        have_header = False
        first_timestamp_ms = 0.0
        for line in f:
            line = line.rstrip('\n')
            if not have_header:
                header_cols = line.split(',')
                have_header = True
            else:
                fields = line.split(',')
                data = {}
                for i, header_col in enumerate(header_cols):
                    data[header_col] = fields[i]
                timestamp_str = data['tpep_pickup_datetime']
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                timestamp_ms = int(dt.timestamp() * 1000)
                if first_timestamp_ms <= 0.0:
                    first_timestamp_ms = timestamp_ms
                delta_ms = timestamp_ms - first_timestamp_ms
                data['delta_ms'] = delta_ms
                yield data

def single_generator_process(options):
    url = options.gateway_url
    t0_ms = int(time() * 1000)
    filename = options.filename
    print('filename=%s' % (filename))
    generator = playback_recorded_file_generator(filename)

    for data in generator:
        try:
            print('generated: ' + str(data))
            data_delta_ms = data['delta_ms']
            speed_up = 10.0
            data_timestamp_ms = t0_ms + data_delta_ms/speed_up
            data['timestamp'] = data_timestamp_ms
            sleep_sec = data_timestamp_ms/1000.0 - time()
            if sleep_sec > 0.0:
                print('sleeping for %s sec' % sleep_sec)
                sleep(sleep_sec)
            print(str(data))
            duplicate_messages = 3
            for i in range(duplicate_messages):
                response = requests.post(url, json=data)
                print(str(response))
                response.raise_for_status()
        except Exception as e:
            print('ERROR: ' + str(e))
            pass

def main():
    parser = OptionParser()
    parser.add_option('-g', '--gateway-url', default='http://localhost:3000/data',
        action='store', dest='gateway_url', help='URL for gateway')
    parser.add_option('-f', '--file',
        action='store', dest='filename', help='Input file')
    options, args = parser.parse_args()

    single_generator_process(options)

if __name__ == '__main__':
    main()
