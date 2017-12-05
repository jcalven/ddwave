#!/usr/bin/env python

import pandas as pd
import numpy as np
import zipfile
import pickle
import zlib
from tqdm import tqdm

# @nb.jit(nopython=True)
def get_pulse(pulses, event_number):
    #for event in events:
    n_pulses = len(pulses)
    if n_pulses < 1:
        return None
    pulse_data = {"left": np.zeros(n_pulses, np.int),
                  "right": np.zeros(n_pulses, np.int),
                  "length": np.zeros(n_pulses, np.int),
                  "max_waveform_length": np.zeros(n_pulses, np.int),
                  "event_number": np.zeros(n_pulses, np.int),
                  "channel": np.zeros(n_pulses, np.int),
                  "waveform": [],
                  "baseline": []}
    
    for i, pulse in enumerate(pulses):
        pulse_data["left"][i] = pulse.left
        pulse_data["right"][i] = pulse.right
        pulse_data["length"][i] = pulse.length
        pulse_data["max_waveform_length"][i] = len(pulse.raw_data)
        pulse_data["event_number"][i] = event_number
        pulse_data["channel"][i] = pulse.channel
        pulse_data["waveform"].append(pulse.raw_data)
        pulse_data["baseline"].append(pulse.baseline)

    return pd.DataFrame(pulse_data)

# @nb.jit(nopython=True)
def get_pulses(events):
    n_events = len(events)
    max_waveforms_lengths = []
    for i in tqdm(range(n_events)):
        max_waveforms_lengths.append(get_pulse(events[i].pulses, i))
    return pd.concat(max_waveforms_lengths)
    
# @nb.jit(nopython=True)
def make_event(pulse, left, right, full_event=None):
    full_event[left:right+1] += pulse
    return full_event

# @nb.jit(nopython=True)
def get_full_event(event, N_PMTS=248, EVENT_SIZE=int(2e5)):
    waveforms = np.zeros((EVENT_SIZE, N_PMTS), dtype=np.float64)
    for i, (channel, left, right, pulse) in enumerate(zip(event.channel, event.left, event.right, event.waveform)):
        try:
            waveforms[left:right+1, channel] += pulse
        except ValueError:
            print("left = {} and right = {} exceeds the event size array of size = {}".format(left, right, EVENT_SIZE))
    event_number = event.event_number.loc[0]
    event_out = pd.DataFrame(waveforms, columns=["channel_{}".format(i) for i in range(N_PMTS)])
    event_out["event_number"] = event_number
    return event_out