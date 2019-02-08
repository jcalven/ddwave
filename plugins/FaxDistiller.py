#!/usr/bin/env python

from plugins import FaxIO, ReconFaxWaveform


class Condensate(object):
    
    """
        Class of distilled FAX events.
        
        Methods:
            : get_events() : Returns pandas.DataFrame with core information on pulses in the event.
            : get_waveforms_in_channel() : Returns pandas.DataFrame with reconstructed waveforms per
                                           channel (pmt) for the event.
            : get_event_truth() : Returns pandas.DataFrame with FAX truth information on the event
    """
    
    def __init__(self, pax_event=None, event=None, waveforms_in_channels=None, event_truth=None, event_instructions=None):
        
        self._pax_event = pax_event
        self._event = event
        self._waveforms_in_channels = waveforms_in_channels
        self._event_truth = event_truth
        self._event_instructions = event_instructions
        
    def get_pax_event(self):
        """Returns pandas.DataFrame with core information on pulses in the event"""
        return self._pax_event
        
    def get_event(self):
        """Returns pandas.DataFrame with core information on pulses in the event"""
        return self._event
    
    def get_waveforms_in_channels(self):
        """Returns pandas.DataFrame with reconstructed waveforms per channel (pmt) 
           for the event"""
        return self._waveforms_in_channels
    
    def get_event_truth(self):
        """Returns pandas.DataFrame with FAX truth information on the event"""
        return self._event_truth
    
    def get_event_instructions(self):
        """Returns pandas.DataFrame with FAX truth information on the event"""
        return self._event_instructions
    
    

class Distill(object):
    
    N_PMTS = 248
    EVENT_SIZE = int(3.5e5)
    
    def __init__(self, zipfile=None, truth_file=None, instructions_file=None):
        
        self.zipfile = zipfile
        self.truth_file = truth_file
        self.instructions_file = instructions_file
        self.events = None
        self.pulses = None
        try:
            self.truth = FaxIO.LoadCSV(self.truth_file)
        except:
            print("No truth file was loaded.")
            self.truth = None
        try:
            self.instructions = FaxIO.LoadCSV(self.instructions_file)
        except:
            print("No instructions file was loaded.")
            self.instructions = None
        
    def open_zip(self, zipfile=None):
        if zipfile is None:
            zipfile = self.zipfile
        if zipfile is not None:
            zipfile_stream = FaxIO.ReadZipped()
            zipfile_stream.open(zipfile)
            return zipfile_stream
        else:
            raise ValueError("Must have a valid zip file to process.")
        
    def close_zip(self, zipfile_stream):
        zipfile_stream.close()
        
    def load_event(self, zipfile_stream, event_i=0, pulse_properties=True, event_numbers=None, **kwargs):
        event = FaxIO.run_stream(zipfile_stream, pulse_properties=pulse_properties, event_i=event_i, event_numbers=event_numbers)
        pulses = ReconFaxWaveform.get_pulses_in_event(event)
        return event, pulses
        
    def load(self, pulse_properties=True, **kwargs):
        # Read pax events from zip file
        self.events = FaxIO.run(self.zip_file, pulse_properties=pulse_properties)
        # Get some pax.event attributes (waveform, channel, etc.) from each pax event
        self.pulses = ReconFaxWaveform.get_pulses(self.events)
        
    def _get_event_data(self, event_number):
        event = self.pulses.query("event_number == {}".format(event_number))
        waveforms_in_channels = ReconFaxWaveform.get_full_event(event, N_PMTS=self.N_PMTS, EVENT_SIZE=self.EVENT_SIZE)
        if self.truth is not None:
            event_truth = self.truth.query("instruction == {}".format(event_number))
        else:
            event_truth = None
        if self.instructions is not None:
            event_instructions = self.instructions.query("instruction == {}".format(event_number))
        else:
            event_instructions = None
        return Condensate(event, waveforms_in_channels, event_truth, event_instructions)
    
    def _get_event_data_stream(self, pax_event, pulses):
        waveforms_in_channels = ReconFaxWaveform.get_full_event(pulses, N_PMTS=self.N_PMTS, EVENT_SIZE=self.EVENT_SIZE)
        if self.truth is not None:
            event_truth = self.truth.query("instruction == {}".format(pax_event.event_number))
        else:
            event_truth = None
        if self.instructions is not None:
            event_instructions = self.instructions.query("instruction == {}".format(pax_event.event_number))
        else:
            event_instructions = None
        return Condensate(pax_event, pulses, waveforms_in_channels, event_truth, event_instructions)
    
    def get(self, n_events):
        """Returns an iterator of tuples containing:
                : event : DataFrame with event information
                : pmt_waveforms : DataFrame with reconstructed waveforms per PMT channel
                : event_truth : DataFrame with event truth information
           for first number of events n_events among all events loaded.
           
           Input:
                : n_events : The number of events to return (Int)
           Output:
                : Iterator of Condensate objects
        """
        
        event_numbers = self.pulses.event_number.unique()
        i = 1
        while i <= n_events and i < len(event_numbers): 
            yield(self._get_event_data(event_numbers[i-1]))
            i += 1
            
    def get_event(self, n_events, pulse_properties=True, zipfile=None, **kwargs):
        """Returns an iterator of tuples containing:
                : event : DataFrame with event information
                : pmt_waveforms : DataFrame with reconstructed waveforms per PMT channel
                : event_truth : DataFrame with event truth information
           for first number of events n_events among all events loaded.
           
           Input:
                : n_events : The number of events to return (Int)
           Output:
                : Iterator of Condensate objects
        """
        zipfile_stream = self.open_zip(zipfile=zipfile)
        event_numbers = zipfile_stream.get_event_numbers_in_current_file()
        i = 0
        while i < n_events and i < len(event_numbers):
            pax_event, pulses = self.load_event(zipfile_stream=zipfile_stream,
                                                event_i=i, pulse_properties=pulse_properties, 
                                                event_numbers=event_numbers, **kwargs)
            
            if pulses is None:
                print("---> Event {} does not have any pulses. Skipping...".format(event_numbers[i]))
            else:
                yield(self._get_event_data_stream(pax_event, pulses))
            i += 1
        self.close_zip(zipfile_stream)