#!/usr/bin/env python
#
# Read JSON data and send to the REST gateway.
# Input files must be sorted by timestamp.
#
# Written by: claudio.fahey@dell.com
#

from __future__ import division
from __future__ import print_function
from time import sleep, time
import requests
from optparse import OptionParser
import json
import pandas as pd

def playback_recorded_file_generator(filenames, period_freq, offset_minutes):
    # Look at the first lines of each file.
    # The first line of the first file will determine the starting period.
    # Then we find the first file that contains a timestamp beyond the desired playback time.
    initialized = False
    start_playback_file_index = None
    previous_file_original_timestamp = None
    for file_index, filename in enumerate(filenames):
        with open(filename, 'r') as f:
            line = f.readline()
            data = json.loads(line)
            original_timestamp = pd.to_datetime(data['timestamp'], unit='ms', utc=True)
            print('File Header: file_index=%d, original_timestamp=%s, filename=%s, ' % (file_index, original_timestamp, filename))

            if previous_file_original_timestamp is not None:
                if not previous_file_original_timestamp <= original_timestamp:
                    raise Exception('Event timestamps are out of order; filename=%s' % filename)
                previous_file_original_timestamp = original_timestamp

            if not initialized:
                first_original_timestamp = original_timestamp
                print('first_original_timestamp=%s' % first_original_timestamp)
                first_period = pd.Period(first_original_timestamp, period_freq)
                begin_playback_period = first_period + 1
                realtime_start_timestamp = pd.Timestamp.now('UTC')
                realtime_start_period = pd.Period(realtime_start_timestamp, period_freq)
                delta_periods = realtime_start_period - begin_playback_period
                original_to_realtime_delta = (
                    delta_periods * realtime_start_period.freq.delta + pd.Timedelta(minutes=offset_minutes))
                print('original_to_realtime_delta=%s' % (original_to_realtime_delta))
                original_timestamp_start_playback = pd.Timestamp.now('UTC') - original_to_realtime_delta
                print('original_timestamp_start_playback=%s' % original_timestamp_start_playback)
                initialized = True

            if start_playback_file_index is None:
                realtime_timestamp = original_timestamp + original_to_realtime_delta
                now_timestamp = pd.Timestamp.now('UTC')
                if realtime_timestamp > now_timestamp:
                    print('First timestamp after now found in file_index=%d' % file_index)
                    start_playback_file_index = file_index - 1
                    if start_playback_file_index < 0:
                        start_playback_file_index = 0
                    # break

    # Open the starting file identified above, fast forward through it to the exact timestamp that matches
    # the current time, then begin playback.
    file_index = start_playback_file_index
    playback_started = False
    skipped_lines = 0
    previous_file_original_timestamp = None
    while file_index < len(filenames):
        filename = filenames[file_index]
        print('Reading file %s' % filename)
        with open(filename, 'r') as f:
            for line_index, line in enumerate(f):
                line = line.rstrip('\n')
                data = json.loads(line)
                original_timestamp = pd.to_datetime(data['timestamp'], unit='ms', utc=True)

                if previous_file_original_timestamp is not None:
                    if not previous_file_original_timestamp <= original_timestamp:
                        raise Exception('Event timestamps are out of order; filename=%s, line=%d' % (filename, line_index+1))
                    previous_file_original_timestamp = original_timestamp

                realtime_timestamp = original_timestamp + original_to_realtime_delta

                if not playback_started:
                    now_timestamp = pd.Timestamp.now('UTC')
                    if realtime_timestamp < now_timestamp:
                        if skipped_lines % 10000 == 0:
                            print('Fast forwarding %s. Event time difference is %s. Skipped %d events.' % (filename, now_timestamp - realtime_timestamp, skipped_lines))
                        skipped_lines += 1
                        continue
                    print('Playback started; skipped_lines=%d; original_timestamp=%s, data=%s' % (skipped_lines, original_timestamp, data))
                    playback_started = True

                data['original_timestamp'] = int(original_timestamp.value / 1e6)
                data['timestamp'] = int(realtime_timestamp.value / 1e6)
                # data['original_timestamp_str'] = str(original_timestamp.tz_convert('Etc/GMT+5'))
                # data['timestamp_str'] = str(realtime_timestamp.tz_convert('Etc/GMT+5'))
                del data['timestamp_str']

                # Format location for Elasticseach.
                data['location'] = {'lat': data['lat'], 'lon': data['lon']}
                del data['lat']
                del data['lon']

                yield data
        file_index += 1

def single_generator_process(filenames, options):
    url = options.gateway_url
    print('num_events=%d' % options.num_events)
    generator = playback_recorded_file_generator(
        filenames, period_freq=options.period_freq, offset_minutes=options.offset_minutes)
    num_events_sent = 0

    for data in generator:
        try:
            print('Generated: ' + str(data))

            timestamp_ms = data['timestamp']
            sleep_sec = timestamp_ms/1000.0 - time()
            if sleep_sec > 0.0:
                print('Sleeping for %s sec' % sleep_sec)
                sleep(sleep_sec)
            backlog_sec = -sleep_sec
            if backlog_sec > 10.0:
                raise Exception('Dropping event that is too old; Backlog %0.3f sec' % backlog_sec)

            duplicate_messages = 1
            for i in range(duplicate_messages):
                response = requests.post(url, json=data)
                print('Response %s, Backlog %0.3f sec' % (str(response), backlog_sec))
                response.raise_for_status()
                num_events_sent += 1
                if options.num_events > 0 and num_events_sent >= options.num_events:
                    return

        except Exception as e:
            print('ERROR: ' + str(e))
            pass

def validate_files(filenames):
    previous_file_original_timestamp = None
    for file_index, filename in enumerate(filenames):
        with open(filename, 'r') as f:
            for line_index, line in enumerate(f):
                data = json.loads(line)
                original_timestamp = pd.to_datetime(data['timestamp'], unit='ms', utc=True)
                if line_index == 0:
                    print('File Header: file_index=%d, original_timestamp=%s, filename=%s, ' % (file_index, original_timestamp, filename))

                if previous_file_original_timestamp is not None:
                    if not previous_file_original_timestamp <= original_timestamp:
                        raise Exception('Event timestamps are out of order; filename=%s' % filename)
                    previous_file_original_timestamp = original_timestamp

def main():
    parser = OptionParser()
    parser.add_option(
        '-g', '--gateway-url', default='http://localhost:3000/data',
        action='store', dest='gateway_url', help='URL for gateway')
    parser.add_option(
        '-f', '--period_freq', default='D',
        action='store', dest='period_freq', help='Align playback and historical timestamps by this period. (D=day,W=week')
    parser.add_option(
        '-o', '--offset_minutes', default=0, type='float',
        action='store', dest='offset_minutes', help='original timestamp will be shifted by this additional amount of minutes')
    parser.add_option(
        '-n', '--num_events', default=0, type='int',
        action='store', dest='num_events', help='number of events to send (0=unlimited)')
    parser.add_option(
        '', '--validate', default=False,
        action='store_true', dest='validate', help='validate files')
    options, args = parser.parse_args()

    if options.validate:
        validate_files(args)
    else:
        single_generator_process(args, options)

if __name__ == '__main__':
    main()
