#!/usr/bin/env python
#
# Simulate brain sensor data and send to the REST gateway.
#
# Written by: claudio.fahey@dell.com
#

from __future__ import division
from __future__ import print_function
import random
from math import sin, pi
from time import sleep, time
import requests
from optparse import OptionParser
from multiprocessing import Process

def sine_wave_generator(att_low, att_high, att_period_ms, att_offset_ms, report_period_sec, device_id, t0_ms):
    t = t0_ms
    while True:
        # att_normalized will be between 0 and 1
        att_normalized = 0.5 * sin(2.0 * pi * (t - att_offset_ms) / att_period_ms) + 0.5
        att = int((att_high - att_low) * att_normalized + att_low)
        assert att >= 0 and att <= 100
        data = {'timestamp': t, 'device_id': device_id, 'att': att, 'ap': 1}
        yield data
        t += 1000

def playback_recorded_file_generator(filename, device_id, t0_ms):
    with open(filename, 'r') as f:
        for line in f:
            delta_t, att = line.split(',')
            delta_t = int(delta_t)
            att = float(att)
            t = t0_ms + delta_t
            data = {'timestamp': t, 'device_id': device_id, 'att': att, 'ap': 1}
            yield data

def single_generator_process(options, device_id):
    url = options.gateway_url
    t0_ms = int(time() * 1000)

    if options.recorded:
        filename = 'cleaned_data_%s.csv' % device_id
        print('%s: filename=%s' % (device_id, filename))
        generator = playback_recorded_file_generator(filename, device_id, t0_ms)
    else:
        # Create random parameters for this device.
        att_low = random.uniform(0.0, 50.0)
        att_high = random.uniform(50.0, 100.0)
        att_period_ms = random.uniform(30000.0, 120000.0)
        att_offset_ms = random.uniform(0.0, 1000000.0)
        report_period_sec = random.uniform(0.75, 1.5)
        print('%s: att_low=%f, att_high=%f, att_period_ms=%f, att_offset_ms=%f, report_period_sec=%f' %
              (device_id, att_low, att_high, att_period_ms, att_offset_ms, report_period_sec))
        generator = sine_wave_generator(att_low, att_high, att_period_ms, att_offset_ms, report_period_sec, device_id, t0_ms)

    for data in generator:
        try:
            t = data['timestamp']
            sleep_sec = t/1000.0 - time()
            if sleep_sec > 0.0:
                sleep(sleep_sec)
            print(device_id + ': ' + str(data))
            response = requests.post(url, json=data)
            print(device_id + ': ' + str(response))
            response.raise_for_status()
        except Exception as e:
            print(device_id + ': ERROR: ' + str(e))
            pass

def main():
    parser = OptionParser()
    parser.add_option('-g', '--gateway-url', default='http://localhost:3000/data',
        action='store', dest='gateway_url', help='URL for gateway')
    parser.add_option('-n', '--num-devices', type='int', default=1,
        action='store', dest='num_devices', help='Number of devices to simulate')
    parser.add_option('-r', '--recorded', default=False,
                      action='store_true', dest='recorded')
    options, args = parser.parse_args()

    device_ids = ['%04d' % (i + 1) for i in range(options.num_devices)]
    while True:
        processes = [
            Process(target=single_generator_process, args=(options, device_id))
            for device_id in device_ids]
        [p.start() for p in processes]
        [p.join() for p in processes]

if __name__ == '__main__':
    main()
