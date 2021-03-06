#!/usr/bin/env python

import numpy as np

from pax import plugin

class PulseProperties(object):
    """Compute pulse properties such as the baseline and noise level.
    Optionally, deletes the left and right extreme ends of the pulses in large events to reduce datasets.
    Note the raw data of the pulse is modified: the computed baseline _is_ subtracted (unless transform_raw=False)
    If the raw data already has the pulse properties pre-computed, no action is taken.
    """
    warning_given = False
    config = {"digitizer_reference_baseline": 16000, 
              "baseline_samples": 47,
              "shrink_data_threshold": float('inf')}
    
    def __init__(self, config=None, transform_raw=True):
        if config is not None:
            self.config = config
        self.transform_raw = transform_raw

    def transform_event(self, event, transform_raw=None):
        if transform_raw is not None:
            self.transform_raw = transform_raw
            
        # Local variables are marginally faster to access in inner loop, so we don't put these in startup.
        reference_baseline = self.config['digitizer_reference_baseline']

        n_baseline = self.config.get('baseline_samples', 50)

        shrink_data_threshold = self.config.get('shrink_data_threshold', float('inf'))
        shrink_data_samples = self.config.get('shrink_data_samples', n_baseline)

        n_pulses = len(event.pulses)
        warning_given = self.warning_given

        for pulse_i, pulse in enumerate(event.pulses):
            if not np.isnan(pulse.minimum):
                if not warning_given:
                    self.warning_given = True
                return event

            # Retrieve waveform as floats: needed to subtract baseline (which can be in between ADC counts)
            w = pulse.raw_data.astype(np.float64)

            # Subtract reference baseline, invert (so hits point up from baseline)
            # This is convenient so we don't have to reinterpret min, max, etc
            w = reference_baseline - w
            
            _results = compute_pulse_properties(w, n_baseline)
            pulse.baseline, pulse.baseline_increase, pulse.noise_sigma, pulse.minimum, pulse.maximum = _results
            
            # Set the waveform to the modified waveform
            if self.transform_raw:
                pulse.raw_data = w - pulse.baseline

            if n_pulses > shrink_data_threshold:
                # Remove the start and end of each pulse, which don't contain much useful information, but
                # takes up a lot of space
                pulse.raw_data = pulse.raw_data[shrink_data_samples:-shrink_data_samples]
                pulse.right -= n_baseline
                pulse.left += n_baseline
        return event


@numba.jit(numba.typeof((1.0, 1.0, 1.0, 1.0, 1.0))(numba.float64[:], numba.int64),
           nopython=True)
def compute_pulse_properties(w, baseline_samples):
    """Compute basic pulse properties quickly
    :param w: Raw pulse waveform in ADC counts
    :param baseline_samples: number of samples to use for baseline computation at the start of the pulse
    :return: (baseline, baseline_increase, noise_sigma, min, max);
      baseline is the average of the first baseline_samples in the pulse
      baseline_increase = baseline_after - baseline_before
      min and max relative to baseline
      noise_sigma is the std of samples below baseline
    Does not modify w. Does not assume anything about inversion of w!!
    """
    # Compute the baseline before and after the self-trigger
    baseline = 0.0
    baseline_samples = min(baseline_samples, len(w))
    for x in w[:baseline_samples]:
        baseline += x
    baseline /= baseline_samples

    baseline_after = 0.0
    for x in w[-baseline_samples:]:
        baseline_after += x
    baseline_after /= baseline_samples

    baseline_increase = baseline_after - baseline

    # Now compute mean, noise, and min
    n = 0           # Running count of samples included in noise sample
    m2 = 0          # Running sum of squares of differences from the baseline
    max_a = -1.0e6  # Running max amplitude
    min_a = 1.0e6   # Running min amplitude

    for x in w:
        if x > max_a:
            max_a = x
        if x < min_a:
            min_a = x
        if x < baseline:
            delta = x - baseline
            n += 1
            m2 += delta*(x-baseline)

    if n == 0:
        # Should only happen if w == baseline everywhere
        noise = 0
    else:
        noise = (m2/n)**0.5

    return baseline, baseline_increase, noise, min_a - baseline, max_a - baseline
