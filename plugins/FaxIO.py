#!/usr/bin/env python

import pandas as pd
import numpy as np
import zipfile
import pickle
import zlib
from tqdm import tqdm

class ReadZipped(object):
    """Read a folder of zipfiles containing [some format]
    Should be followed by a decoder plugin who will decompress and decode the events.
    It's better to split this task up, since input is single-core only,
    while encoding & compressing can still be done by the processing workers
    """
    do_output_check = False
    file_extension = 'zip'

    def open(self, filename):
        import zipfile
        self.current_file = zipfile.ZipFile(filename)
        self.event_numbers = sorted([int(x)
                                     for x in self.current_file.namelist()])

    def get_event_numbers_in_current_file(self):
        return self.event_numbers

    def get_single_event_in_current_file(self, event_number):
        with self.current_file.open(str(event_number)) as event_file_in_zip:
            data = event_file_in_zip.read()
            return data

    def close(self):
        """Close the currently open file"""
        self.current_file.close()
        
        
class LoadEvent(object):
    
    def __new__(cls, data=None):
        if data is not None:
            event = cls.load(cls.decompress(data))
            return event
        
    @classmethod
    def decompress(cls, data):
        return zlib.decompress(data)
    
    @classmethod
    def load(cls, data):
        return pickle.loads(data)
    

class LoadCSV(object):
    
    def __new__(cls, filename=None):
        if filename is not None:
            return cls.load(filename)
    
    @classmethod    
    def load(cls, filename):
        return pd.read_csv(filename)
    
    
# For multiprocessing reading/unzipping PAX raw date. 
# NOT IN USE!
def process(n=1, zipfile=None, event_numbers=None):
 
    start_time = time.time()

    with multiprocessing.Pool(processes=n) as pool:
        results = pool.map_async(get_event, (zipfile, event_numbers))
        results.wait()
        print(results.get(timeout=1))

    end_time = time.time()
    return results
    
def get_event(zipfile, event_number):
    event = LoadEvent(zipfile.get_single_event_in_current_file(event_number))
    return event

def run(file, n=1):
    """Retrieve pax Event objects for all events"""
    events = []
    zipfile = ReadZipped()
    zipfile.open(file)
    
    event_numbers = zipfile.get_event_numbers_in_current_file()
    
    for ev in tqdm(event_numbers):
        events.append(get_event(zipfile, ev))
    return events