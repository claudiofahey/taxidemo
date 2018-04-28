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
import argparse
import json
import pandas as pd
import glob
from requests import Session
from requests_futures.sessions import FuturesSession
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def playback_recorded_file_generator(filenames, period_freq, offset_minutes, start_at_first_line, fast_forward_from_minutes_ago):
    # Look at the first lines of each file.
    # The first line of the first file will determine the starting period.
    # Then we find the first file that contains a timestamp beyond the desired playback time.
    initialized = False
    start_playback_file_index = None
    previous_file_original_timestamp = None
    playback_started = False
    fast_forward_delta = pd.Timedelta(minutes=fast_forward_from_minutes_ago)
    for file_index, filename in enumerate(filenames):
        with open(filename, 'r') as f:
            line = f.readline()
            data = json.loads(line)
            original_timestamp = pd.to_datetime(data['timestamp'], unit='ms', utc=True)
            print('File Header: file_index=%d, original_timestamp=%s, filename=%s, ' % (file_index, original_timestamp, filename))

            if start_at_first_line:
                start_playback_file_index = file_index
                # startup_delay = pd.Timedelta(seconds=1)
                original_to_realtime_delta = pd.Timestamp.now('UTC') - fast_forward_delta - original_timestamp
                playback_started = True
                break

            if previous_file_original_timestamp is not None:
                if not previous_file_original_timestamp <= original_timestamp:
                    raise Exception('Event timestamps are out of order; filename=%s' % filename)
                previous_file_original_timestamp = original_timestamp

            if not initialized:
                first_original_timestamp = original_timestamp
                print('first_original_timestamp=%s' % first_original_timestamp)
                first_period = pd.Period(first_original_timestamp, period_freq)
                begin_playback_period = first_period + 1
                realtime_start_timestamp = pd.Timestamp.now('UTC') - fast_forward_delta
                realtime_start_period = pd.Period(realtime_start_timestamp, period_freq)
                delta_periods = realtime_start_period - begin_playback_period
                original_to_realtime_delta = (
                    delta_periods * realtime_start_period.freq.delta + pd.Timedelta(minutes=offset_minutes))
                print('original_to_realtime_delta=%s' % (original_to_realtime_delta))
                original_timestamp_start_playback = pd.Timestamp.now('UTC') - fast_forward_delta - original_to_realtime_delta
                print('original_timestamp_start_playback=%s' % original_timestamp_start_playback)
                initialized = True

            if start_playback_file_index is None:
                realtime_timestamp = original_timestamp + original_to_realtime_delta
                now_timestamp = pd.Timestamp.now('UTC') - fast_forward_delta
                if realtime_timestamp > now_timestamp:
                    print('First timestamp after now found in file_index=%d' % file_index)
                    start_playback_file_index = file_index - 1
                    if start_playback_file_index < 0:
                        start_playback_file_index = 0
                    # break

    # Open the starting file identified above, fast forward through it to the exact timestamp that matches
    # the current time, then begin playback.
    file_index = start_playback_file_index
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
                    now_timestamp = pd.Timestamp.now('UTC') - fast_forward_delta
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

                # Format location for Elasticsearch.
                data['location'] = {'lat': data['lat'], 'lon': data['lon']}
                del data['lat']
                del data['lon']

                yield data
        file_index += 1

def chunks(l, n):
    """Yield successive n-sized chunks from l.
    From https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks"""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def single_generator_process(filenames, options):
    url = options.gateway_url
    print('num_events=%d' % options.num_events)
    generator = playback_recorded_file_generator(
        filenames,
        period_freq=options.period_freq,
        offset_minutes=options.offset_minutes,
        start_at_first_line=options.start_at_first_line,
        fast_forward_from_minutes_ago=options.fast_forward_from_minutes_ago)
    num_events_sent = 0

    if options.concurrency <= 1:
        session = Session()
    else:
        executor = ThreadPoolExecutor(max_workers=options.concurrency)
        # executor = ProcessPoolExecutor(max_workers=options.concurrency)
        session = FuturesSession(executor=executor, session=Session())

    data_queue = []
    last_report_time = time()
    last_reported_events = 0

    for data in generator:
        try:
            if options.num_events > 0 and num_events_sent >= options.num_events:
                print('Finished sending requested number of events.')
                return

            print('Generated: ' + str(data))
            data_queue.append(data)
            timestamp_ms = data['timestamp']
            sleep_sec = timestamp_ms/1000.0 - time()
            backlog_sec = -sleep_sec

            # If we need to sleep or the queue is at the maximum size, send queued events in parallel.
            if len(data_queue) > 0 and (sleep_sec > 0.0 or len(data_queue) >= options.batch_size):
                print('Sending %d queued events' % len(data_queue))
                num_events_sent += len(data_queue)
                responses = [session.post(url, json=d) for d in chunks(data_queue, 128)]
                data_queue = []
                if options.concurrency > 1:
                    responses = [f.result() for f in responses]
                # responses = [f.result for f in futures]
                # print('len(futures)=%d' % len(futures))
                # futures = [session.post(url, json=data_queue)]
                for response in responses:
                    print('Response %s, Backlog %0.3f sec' % (str(response), backlog_sec))
                    response.raise_for_status()

            sleep_sec = timestamp_ms/1000.0 - time()
            if sleep_sec > 0.0:
                print('Sleeping for %s sec' % sleep_sec)
                sleep(sleep_sec)

            time_now = time()
            if last_report_time + 3.0 < time_now:
                events_per_sec = (num_events_sent - last_reported_events) / (time_now - last_report_time)
                print('events_per_sec=%0.2f' % events_per_sec)
                last_report_time = time_now
                last_reported_events = num_events_sent

        except Exception as e:
            print('ERROR: ' + str(e))
            pass
            # return

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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-g', '--gateway-url', default='http://localhost:3000/data',
        action='store', dest='gateway_url', help='URL for gateway')
    parser.add_argument(
        '--start-at-first-line', default=False,
        action='store_true', dest='start_at_first_line', help='Do not align timestamps. Start playback at first line of first file.')
    parser.add_argument(
        '-f', '--period-freq', default='D',
        action='store', dest='period_freq', help='Align playback and historical timestamps by this period. (D=day,W=week)')
    parser.add_argument(
        '-o', '--offset-minutes', default=0.0, type=float,
        action='store', dest='offset_minutes', help='original timestamp will be shifted by this additional amount of minutes')
    parser.add_argument(
        '-n', '--num-events', default=0, type=int,
        action='store', dest='num_events', help='number of events to send (0=unlimited)')
    parser.add_argument(
        '--validate', default=False,
        action='store_true', dest='validate', help='validate files')
    parser.add_argument(
        '--fast-forward-from-minutes-ago', default=0.0, type=float,
        action='store', dest='fast_forward_from_minutes_ago', help='')
    parser.add_argument(
        '--concurrency', default=2, type=int,
        action='store', dest='concurrency', help='')
    parser.add_argument(
        '--batch-size', default=256, type=int,
        action='store', dest='batch_size', help='')
    parser.add_argument(
        '--chunk-size', default=128, type=int,
        action='store', dest='chunk', help='')
    options, unparsed = parser.parse_known_args()

    filenames = unparsed
    filenames = sorted([p for s in filenames for p in glob.glob(s)])

    if options.validate:
        validate_files(filenames)
    else:
        single_generator_process(filenames, options)

if __name__ == '__main__':
    main()
