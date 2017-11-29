# import plugins.FaxIO as faxio
# import plugins.ReconFaxWaveform
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
    
    def __init__(self, event=None, waveforms_in_channels=None, event_truth=None):
        
        self._event = event
        self._waveforms_in_channels = waveforms_in_channels
        self._event_truth = event_truth
        
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
    
    

class Distill(object):
    
    N_PMTS = 248
    EVENT_SIZE = int(2e5)
    
    def __init__(self, zip_file=None, truth_file=None):
        
        self.zip_file = zip_file
        self.truth_file = truth_file
        self.events = None
        self.pulses = None
        self.truth = None
        
    def load(self):
        # Read pax events from zip file
        self.events = FaxIO.run(self.zip_file)
        # Get some pax.event attributes (waveform, channel, etc.) from each pax event
        self.pulses = ReconFaxWaveform.get_pulses(self.events)
        # Get fax truth for all events
        self.truth = FaxIO.LoadTruth(self.truth_file)
        
    def _get_event_data(self, event_number):
        event = self.pulses.query("event_number == {}".format(event_number))
        waveforms_in_channels = ReconFaxWaveform.get_full_event(event, N_PMTS=self.N_PMTS, EVENT_SIZE=self.EVENT_SIZE)
        event_truth = self.truth.query("event == {}".format(event_number))
        return Condensate(event, waveforms_in_channels, event_truth)
    
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
        while i <= n_events:
            yield(self._get_event_data(event_numbers[i]))
            i += 1