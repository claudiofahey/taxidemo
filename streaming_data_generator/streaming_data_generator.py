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
                rawdata = {}
                for i, header_col in enumerate(header_cols):
                    rawdata[header_col] = fields[i]

                cleandata = {}
                cleandata['tpep_dropoff_timestamp_ms'] = int(datetime.strptime(rawdata['tpep_dropoff_datetime'], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
                cleandata['tpep_pickup_timestamp_ms'] = int(datetime.strptime(rawdata['tpep_pickup_datetime'], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
                if first_timestamp_ms <= 0.0:
                    first_timestamp_ms = cleandata['tpep_dropoff_timestamp_ms']
                delta_ms = cleandata['tpep_dropoff_timestamp_ms'] - first_timestamp_ms
                cleandata['delta_ms'] = delta_ms

                cleandata['pickup_location'] = {
                    'lat': float(rawdata['pickup_latitude']),
                    'lon': float(rawdata['pickup_longitude']),
                }
                cleandata['dropoff_location'] = {
                    'lat': float(rawdata['dropoff_latitude']),
                    'lon': float(rawdata['dropoff_longitude']),
                }

                cleandata['trip_duration_minutes'] = (cleandata['tpep_dropoff_timestamp_ms'] - cleandata['tpep_pickup_timestamp_ms']) / (60 * 1000)
                cleandata['trip_distance'] = float(rawdata['trip_distance'])
                cleandata['passenger_count'] = int(rawdata['passenger_count'])
                cleandata['total_amount'] = float(rawdata['total_amount'])

                yield cleandata

def single_generator_process(options):
    url = options.gateway_url
    t0_ms = int(time() * 1000)
    filename = options.filename
    print('filename=%s' % (filename))
    generator = playback_recorded_file_generator(filename)

    for data in generator:
        try:
            print('generated: ' + str(data))

            data['routing_key'] = '%0.2f,%0.1f' % (data['dropoff_location']['lat'], data['dropoff_location']['lon'])

            data_delta_ms = data['delta_ms']
            speed_up = 10.0
            data_timestamp_ms = t0_ms + data_delta_ms/speed_up
            data['timestamp'] = int(data_timestamp_ms)
            sleep_sec = data_timestamp_ms/1000.0 - time()
            if sleep_sec > 0.0:
                print('sleeping for %s sec' % sleep_sec)
                sleep(sleep_sec)
            elif sleep_sec < -10.0:
                raise Exception('Dropping event that is too old; age=%0.3f sec' % (-sleep_sec))

            print('sending: ' + str(data))
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

    while True:
        single_generator_process(options)

if __name__ == '__main__':
    main()
