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
import json
import pandas as pd
import numpy as np

def playback_recorded_file_generator(filename, period_freq):
    with open(filename, 'r') as f:
        initialized = False
        skipped_lines = 0
        playback_started = False
        for line in f:
            line = line.rstrip('\n')
            data = json.loads(line)

            original_timestamp = pd.to_datetime(data['timestamp'], unit='ms', utc=True)

            if not initialized:
                first_original_timestamp = original_timestamp
                first_period = pd.Period(first_original_timestamp, period_freq)
                begin_playback_period = first_period #+ 1
                # begin_playback_original_timestamp = begin_playback_period.start_time
                realtime_start_timestamp = pd.Timestamp.now('UTC')
                realtime_start_period = pd.Period(realtime_start_timestamp, period_freq)
                delta_periods = realtime_start_period - begin_playback_period
                original_to_realtime_delta = delta_periods * realtime_start_period.freq.delta
                print('original_to_realtime_delta=%s' % (original_to_realtime_delta))
                initialized = True

            realtime_timestamp = original_timestamp + original_to_realtime_delta

            if not playback_started:
                now_timestamp = pd.Timestamp.now('UTC')
                if realtime_timestamp < now_timestamp:
                    if skipped_lines % 10000 == 0:
                        print('Fast forwarding. Event time difference is %s. Skipped %d events.' % (now_timestamp - realtime_timestamp, skipped_lines))
                    skipped_lines += 1
                    continue
                print('Playback started; skipped_lines=%d; original_timestamp=%s, data=%s' % (skipped_lines, original_timestamp, data))
                playback_started = True

            data['original_timestamp'] = int(original_timestamp.value / 1e6)
            data['timestamp'] = int(realtime_timestamp.value / 1e6)
            data['original_timestamp_str'] = str(original_timestamp.tz_convert('US/Eastern'))
            data['timestamp_str'] = str(realtime_timestamp.tz_convert('US/Eastern'))

            data['location'] = {'lat': data['lat'], 'lon': data['lon']}
            del data['lat']
            del data['lon']

            yield data

def single_generator_process(options):
    url = options.gateway_url
    filename = options.filename
    print('filename=%s' % filename)
    print('num_events=%d' % options.num_events)
    generator = playback_recorded_file_generator(filename, period_freq=options.period_freq)
    num_events_sent = 0

    for data in generator:
        try:
            print('generated: ' + str(data))

            timestamp_ms = data['timestamp']  #.value / 1e6
            sleep_sec = timestamp_ms/1000.0 - time()
            if sleep_sec > 0.0:
                print('Sleeping for %s sec' % sleep_sec)
                sleep(sleep_sec)
            elif sleep_sec < -10.0:
                raise Exception('Dropping event that is too old; age=%0.3f sec' % (-sleep_sec))

            duplicate_messages = 1
            for i in range(duplicate_messages):
                response = requests.post(url, json=data)
                print(str(response))
                response.raise_for_status()
                num_events_sent += 1
                if options.num_events > 0 and num_events_sent >= options.num_events:
                    return

        except Exception as e:
            print('ERROR: ' + str(e))
            pass

def main():
    parser = OptionParser()
    parser.add_option('-g', '--gateway-url', default='http://localhost:3000/data',
        action='store', dest='gateway_url', help='URL for gateway')
    parser.add_option('-f', '--file',
        action='store', dest='filename', help='Input file')
    parser.add_option('', '--period_freq', default='D',
                      action='store', dest='period_freq', help='D=day')
    parser.add_option('', '--num_events', default=0, type='int',
                      action='store', dest='num_events', help='number of events to send (0=unlimited)')
    options, args = parser.parse_args()

    # while True:
    single_generator_process(options)

if __name__ == '__main__':
    main()
