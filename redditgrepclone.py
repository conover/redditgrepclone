#! /usr/bin/python
# 
# Copyright (c) 2011 Christopher Conover
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import os
import os.path
import sys
import re
from datetime import datetime, timedelta

class RedditGrepClone(object):
    '''
        A utility to efficiently search a Reddit web server log file for a set
        of logs that match a specified timestamp or range of timestamps.  
    '''
    __version__ = 0.1
        
    # Key dates and times
    # _TODAY can be modified to control which day of the month is assigned
    # to the input pattern
    _TODAY          = datetime.today() 
    _START_OF_TODAY = datetime(_TODAY.year, _TODAY.month, _TODAY.day, 0, 0, 0)
    _END_OF_TODAY   = datetime(_TODAY.year, _TODAY.month, _TODAY.day, 
                                                                23, 59, 59)
    
    # Which log in a list of logs with the same timestamp are we looking for
    _CHASE_FIRST, _CHASE_LAST = 0, 1
    
    # Key log details
    _first_log_dt, _last_log_dt, _last_log_offset = None, None, None
    
    # Set of searches to be performed. Form (start_dt, end_dt)
    _searches = []
    
    # Set of resulting offsets found by searching. 
    # Form (start_offset, end_offset)
    _offsets = []
    
    # Absolute start and end datetimes provided by constructor
    _abs_start_dt, _abs_end_dt = None, None
    
    # That's no range...
    _look_for_exact = False
    
    _file, _file_size = None, None
    
    class ArgumentError(Exception): pass
    class ParseError(Exception): pass
    
    def __del__(self):
        self._file.close()
        
    def __init__(self, *args):
        
        filename = '/logs/haproxy.log' # default filename
        
        if len(args) > 2 or len(args) == 0:
            raise self.ArgumentError, 'Expecting 1 or 2 arguments'
        else:
            if len(args) > 1:
                try:
                    self._abs_start_dt, self._abs_end_dt = \
                                                self._parse_pattern(args[0])
                    filename = args[1]
                except self.ArgumentError:
                    try:
                        self._abs_start_dt, self._abs_end_dt = \
                                                self._parse_pattern(args[1])
                        filename = args[0]
                    except self.ArgumentError:
                        raise self.ArgumentError, \
                '''Neither argument specified is a valid timestamp pattern'''
            else:
                self._abs_start_dt, self._abs_end_dt = \
                                                self._parse_pattern(args[0])
                
        assert isinstance(filename, basestring), 'Filename must be a string'
        self._file = open(filename, 'rb')
        self._file_size = os.path.getsize(filename) - 1
        
        self._findKeyLogs()
        self._defineSearches()
        
    def _findKeyLogs(self):
        '''
            Find first and last log timestamps and offsets
        '''
        # Assume first log starts and offset 0
        self._file.seek(0)
        self._first_log_dt = self._date_at_offset()
        
        self._file.seek(self._file_size)
        self._last_log_dt = self._date_at_offset()
        self._last_log_offset = self._file.tell()
        
        return
        
    def _defineSearches(self):
        '''
            Define a set of searches to be performed. Takes into account
            midnight rollovers.
        '''
        possible_searches = []
        one_day = timedelta(days = 1)
        if self._abs_start_dt > self._abs_end_dt:
            # Spanning a midnight 
            # Ex: 23:50:51-0:00:1 
            # Could be Yesterday 23:50:51 - Today 00:00:01
            # or
            # Could be Today 23:50:51 - Tomorrow 00:00:01
            possible_searches.append((self._abs_start_dt - one_day, 
                                                            self._abs_end_dt))
            possible_searches.append((self._abs_start_dt, 
                                                self._abs_end_dt + one_day))
        else:
            # Intra-day range
            # Ex: 00:00:01-:00:00:05
            # Could be Yesterday 00:00:01 to Yesterday 00:00:05
            # or
            # Could be Today 00:00:01 to Today 00:00:05
            # or
            # Could be Tomorrow 00:00:01 to Tomorrow 00:00:05
            #
            # While the spec specified the overlap would only be a few 
            # minutes, best not to assume.
            possible_searches.append((self._abs_start_dt - one_day, 
                                                self._abs_end_dt - one_day))
            possible_searches.append((self._abs_start_dt, self._abs_end_dt))
            possible_searches.append((self._abs_start_dt + one_day, 
                                                self._abs_end_dt + one_day))
        
        # Filter out searches that can't match anything in this log file
        for search in possible_searches:
            start_dt, end_dt = search
            if end_dt >= self._first_log_dt and start_dt <= self._last_log_dt:
                self._searches.append(search)
        return
        
    def search(self):
        '''
            Finds the offsets of logs within the ranges defined in _searches.
            Returns approximatly how many logs are in the the result sets.
        '''
        for start_dt, end_dt in self._searches:
            start_offset = self._find_offset(start_dt, 0, self._CHASE_FIRST)
            if start_offset is None and self._look_for_exact:
                continue
            end_offset = self._find_offset(end_dt, start_offset, 
                                                            self._CHASE_LAST)
            self._offsets.append((start_offset, end_offset))
        
        return
        
    def __iter__(self):
        '''
            Returns self.next() to facilitate iteration
        '''
        return self.next()
        
    def next(self):
        '''
            Use a generator to iterate over all the logs between each set of
            offsets. 
        '''
        for start_offset, end_offset in self._offsets:
            self._file.seek(start_offset)
            while self._file.tell() <= end_offset + 1:
                yield self._file.readline()
        
    def _find_offset(self, target_ts, lower_bound, mode):
        '''
            Binary search to find the offset of the log with closest possible
            timestamp to the target timestamp.
        '''
        FORWARD_JUMP,BACK_JUMP = 0, 1
        
        prev_seek_offset    = -1
        last_jump           = None
        upper_bound         = self._file_size
        
        while True:
            seek_offset = ((upper_bound - lower_bound) / 2) + lower_bound
            
            if prev_seek_offset == seek_offset:
                # Oscillated back and forth between the same offset two
                # iterations in a row. This is as close as we can get.
                if self._look_for_exact:
                    return
                else:
                    # Could be on wrong side of best guess timestamp after  
                    # last jump
                    if (mode == self._CHASE_FIRST and 
                                                last_jump == FORWARD_JUMP):
                        self._file.readline()
                    elif (mode == self._CHASE_LAST and 
                            last_jump == BACK_JUMP and self._file.tell() > 0):
                        self._file.seek(self._file.tell() - 1)
                        self._date_at_offset()
                    return self._file.tell()
            else:
                prev_seek_offset = seek_offset
            
            # Move the midpoint and check the date  
            self._file.seek(seek_offset)
            seek_ts = self._date_at_offset()
            if seek_ts > target_ts: # Passed it
                upper_bound, last_jump = seek_offset, BACK_JUMP
            elif seek_ts < target_ts: # Before it
                lower_bound, last_jump = seek_offset, FORWARD_JUMP
            else:
                # Make sure this is the first or last log in a list of 
                # potentially many logs with the same timestamp
                if self._file.tell() == 0:
                    return 0
                elif mode == self._CHASE_FIRST:
                    self._file.seek(self._file.tell() - 1)
                    prev_ts = self._date_at_offset()
                    if prev_ts == target_ts:
                        upper_bound = seek_offset
                    else:
                        self._file.seek(seek_offset)
                        self._date_at_offset()
                        return self._file.tell()
                elif mode == self._CHASE_LAST:
                    self._file.readline()
                    next_ts = self._date_at_offset()
                    if next_ts == target_ts:
                        lower_bound = seek_offset
                    else:
                        self._file.seek(seek_offset)
                        self._date_at_offset()
                        return self._file.tell()
                        
    def _date_at_offset(self):
        '''
            Backtrack to find the beginning of the current line and parse
            timestamp. File cursor will be moved to the beginning of the line.
            
            Returns a datetime object of the current line log's timestamp.
        '''
        start_offset, reads, seek_to = self._file.tell(), 0, None
        while True:
            seek_to = start_offset - reads
            self._file.seek(seek_to)
            if seek_to == 0: break # Beginning of file
            char = self._file.read(1)
            # Could have landed directly on the new line. Avoid new lines at
            # the end of the file
            if (char == '\n' and reads > 0 and 
                                    seek_to != self._file_size):
                break
            reads += 1
        
        # Homogenize spacing
        fixed_line = re.sub('\s+', ' ', self._file.readline(), 3)
        try:
            month, day, time, log = fixed_line.split(' ', 3)
    
            # readline() moved the cursor to end of the line, move it back
            # to the beginning
            if seek_to > 0:
                self._file.seek(seek_to + 1)
            else:
                self._file.seek(0)
    
            log_dt = datetime.strptime(' '.join((month, day, time)), 
                                                            '%b %d %H:%M:%S')
            # Handle year rollovers
            if (log_dt.month == 1 and self._TODAY.month == 12):
                # Ex:
                # TODAY is Dec 31 23:50:00 
                # log_dt is Jan 1 00:01:01
                # Happy New Years Day
                return log_dt.replace(year = self._TODAY.year + 1)
            elif (log_dt.month == 12 and self._TODAY.month == 1):
                # Ex:
                # logt_dt is Dec 31 23:50
                # TODAY is Jan 1 00:00:01
                # Happy New Years Eve
                return log_dt.replace(year = self._TODAY.year - 1)
            else:
                return log_dt.replace(year = self._TODAY.year)
        except Exception, e:
            # Any exceptions here mean that there is a log in the wrong 
            # format. So cover all the exceptions and report the offest
            raise self.ParseError, \
                'Unexpected log format at offset %s. Error: %s. Log: %s' % \
                                            (str(seek_to), e, fixed_line)
            
    def _parse_pattern(self, pattern):
        '''
            Parses and validates input pattern.
            @return start and end as datetimes
        '''
        assert isinstance(pattern,basestring), \
                                        'Timestamp pattern must be a string'
        
        start_dt, end_dt = None, None
        
        # Any possible arguement must match this regular expression
        valid_ts = re.compile('''   [0-9]{1,2}:[0-9]{1,2}
                                    ([0-9]{1,2})?
                                    (-[0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2})?)?'''
                            , re.X)
        if valid_ts.match(pattern) is None:
            raise self.ArgumentError, 'Invalid timestamp pattern format'
    
        # Any timestamp matching this regular expresssion is not a wildcard
        precise_ts = re.compile('[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}')
        
        # Ignore midnight and new year rollovers here. Those cases will be 
        # taken care of by looking at the first and last log timestamps later
        dt_date_args = (self._TODAY.year,self._TODAY.month, self._TODAY.day)
        
        if pattern.find('-') > -1: 
            # Two arg range
            start_ts, end_ts = pattern.split('-')
            
            if precise_ts.match(start_ts) is None:
                 # Wildcard start
                hour, minute = start_ts.split(':')
                start_dt = datetime(*dt_date_args + (int(hour), int(minute)))
            else:
                # Exact start
                hour, minute, second = start_ts.split(':')
                start_dt = datetime(*dt_date_args + (int(hour), int(minute), 
                                                                int(second)))
    
            if precise_ts.match(end_ts) is None:
                # Wildcard end
                hour, minute = end_ts.split(':')
                end_dt  = datetime(*dt_date_args + (int(hour), int(minute), 
                                                                        59))
            else:
                # Exact end
                hour, minute, second = end_ts.split(':')
                end_dt = datetime(*dt_date_args + (int(hour), int(minute), 
                                                                int(second)))
        elif precise_ts.match(pattern) is None:
            # One arg range
            hour, minute = pattern.split(':')
            start_dt = datetime(*dt_date_args + (int(hour), int(minute)))
            end_dt = datetime(*dt_date_args + (int(hour), int(minute), 59))
        else:
            # Exact
            hour, minute, second = pattern.split(':')
            start_dt = datetime(*dt_date_args + (int(hour), int(minute), 
                                                                int(second)))
            end_dt = start_dt
            self._look_for_exact = True
        return start_dt, end_dt
        
if __name__ == '__main__':
    
    if len(sys.argv) == 1:
        print 'You must provide at least a timestamp pattern.'
        print 'See README for details.'
    else:
        tgrep = RedditGrepClone(*sys.argv[1:])
        tgrep.search()
        for log in tgrep:
            print log.replace('\n', '')