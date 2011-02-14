import os, os.path, sys, types, re, time
from datetime import datetime
from datetime import timedelta

MONTH_MAP	= {	'jan':1,
			'feb':2,
			'mar':3,
			'apr':4,
			'may':5,
			'jun':6,
			'jul':7,
			'aug':8,
			'sep':9,
			'oct':10,
			'nov':11,
			'dec':12}

TODAY = datetime(2011,2,11)
START_OF_TODAY = datetime(TODAY.year, TODAY.month, TODAY.day, 0, 0, 0)
END_OF_TODAY = datetime(TODAY.year, TODAY.month, TODAY.day, 23, 59, 59)

class RedditGrepClone(object):
	
	_CHASE_FIRST_MODE = 0
	_CHASE_LAST_MODE = 1
	
	
	def __init__(self, filename):
		self.file = open(filename, 'rb')
		self.file_size = os.path.getsize(filename) - 1
		
	def find(self, abs_start_ts, abs_end_ts):
		'''
			Utilize binary search to find the logs matching
			the specified timestamps.
			@return generator of logs
		'''
		# Logs with specific timestamp
		self.specific = False
		if abs_end_ts is None: 
			abs_end_ts = abs_start_ts
			self.specific = True
		
		first_ts = self._date_at_offset() # First timestamp in the file
		self.file.seek(self.file_size)
		last_ts = self._date_at_offset() # Last timestamp in the file
		last_offset = self.file.tell()
		
		searches = [] # Set of timestamp ranges we need to search for. In the form of (start_ts, end_ts)
		offsets = [] # Set of offset ranges need to output
		
		# Possible midnight roll overs. Invalid ranges will be discareded later
		one_day = timedelta(days = 1)
		if abs_start_ts > abs_end_ts: # Ex: 23:50:51-0:00:1
			searches.append((abs_start_ts - one_day, abs_end_ts))
			searches.append((abs_start_ts, abs_end_ts + one_day))
		else:
			searches.append((abs_start_ts, abs_end_ts))
			searches.append((abs_start_ts + one_day, abs_end_ts + one_day))
			searches.append((abs_end_ts - one_day, abs_end_ts - one_day))
		
		for start_ts, end_ts in searches:
			if end_ts < first_ts or start_ts > last_ts:
				# Discard invalid timestamp ranges
				continue
			else:
				
				start_offset = self._find_offset(start_ts, 0, self.file_size, mode = self._CHASE_FIRST_MODE)
				if start_offset is None and specific: 
					continue
				end_offset = self._find_offset(end_ts, start_offset, self.file_size, mode = self._CHASE_LAST_MODE)
				offsets.append((start_offset, end_offset))
			
		for start_offset, end_offset in offsets:
			self.file.seek(start_offset)
			while self.file.tell() <= end_offset + 1:
				yield self.file.readline()
	
	def _find_offset(self, target_ts, lower_bound, upper_bound, mode = 'first', specific = False):
		prev_seek_offset = -1
		while True:
			seek_offset = ((upper_bound - lower_bound) / 2) + lower_bound
			
			if prev_seek_offset == seek_offset:
				# If this happens, we've oscillated back and forth
				# between two different positions incidicating this
				# is as close as we can get
				if self.specific:
					# Specific timestamp not found
					return
				else:
					# It's possible we oscillated to the wrong side of the
					# timestamp we are looking for
					if mode == self._CHASE_FIRST_MODE:
						next_ts = self._date_at_offset()
						if next_ts < target_ts:
							self.file.readline()
					elif mode == self._CHASE_LAST_MODE:
						prev_ts = self._date_at_offset()
						if prev_ts > target_ts:
							self.file.seek(self.file.tell() - 1)
							self._date_at_offset()
					return self.file.tell()
			else:
				prev_seek_offset = seek_offset
			
			# Move the midpoint and check the date	
			self.file.seek(seek_offset)
			seek_ts = self._date_at_offset()
			
			if seek_ts > target_ts: # Passed it
				upper_bound = seek_offset
			elif seek_ts < target_ts: # Before it
				lower_bound = seek_offset
			else:
				# Make sure this is the first or last log in a list of potentially
				# many logs with the same timestamp
				if mode == self._CHASE_FIRST_MODE:
					self.file.seek(self.file.tell() - 1)
					prev_ts = self._date_at_offset()
					if prev_ts == target_ts:
						upper_bound = seek_offset
					else:
						self.file.seek(seek_offset)
						self._date_at_offset()
						return self.file.tell()
				elif mode == self._CHASE_LAST_MODE:
					self.file.readline()
					next_ts = self._date_at_offset()
					if next_ts == end_ts:
						lower_bound = seek_offset
					else:
						self.file.seek(seek_offset)
						self._date_at_offset()
						return self.file.tell()
						
	
	def _date_at_offset(self):
		'''
			Backtrack to find the beginning of the current line and parses
			timestamp. File cursor will be moved to the beginning of the line.
			
			Returns a datetime object of the current line log's timestamp.
		'''
		
		start_offset, reads, seek_to, line = self.file.tell(), 0, None, []
		while True:
			seek_to = start_offset - reads
			self.file.seek(seek_to)
			if seek_to == 0: break # beginning of file
			char = self.file.read(1)
			line.append(char)
			if char == '\n' and reads > 0: # Might have landed on line break
				break
			reads += 1
			
		# Timestamp parts could be delimited by more than one space
		fixed_line = re.sub('\s+', ' ', self.file.readline(), 3)  
		month, day, timestamp, log = fixed_line.split(' ', 3)
		hour, minute, second = timestamp.split(':')
		
		# readline() moved the cursor to end of the line, move it back
		# to the beginning
		self.file.seek(seek_to + 1)
		
		return datetime(TODAY.year, MONTH_MAP[month.lower()], int(day), int(hour), int(minute), int(second))

def parse_timestamp(timestamp):
	assert isinstance(timestamp,types.StringType)
	
	# Any possible arguement must match this regular expression
	valid_ts = '[0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2})?(-[0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2})?)?'
	if re.match(valid_ts, timestamp) is None:
		raise ValueError
	
	# Any timestamp matching this regular expresssion is not a wildcard
	precise_ts = '[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'
	start_dt, end_dt = None, None
	
	if timestamp.find('-') > -1:
		start_ts, end_ts = timestamp.split('-')
	
		if re.match(precise_ts, start_ts) is None:
			hour, minute = start_ts.split(':')
			start_dt = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute))
		else:
			hour, minute, second = start_ts.split(':')
			start_dt = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), int(second))
	
		if re.match(precise_ts, end_ts) is None:
			hour, minute = end_ts.split(':')
			end_dt 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), 59)
		else:
			hour, minute, second = end_ts.split(':')
			end_dt = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), int(second))
	elif re.match(precise_ts, timestamp) is None:
		hour, minute = timestamp.split(':')
		start_dt 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute))
		end_dt 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), 59)
	else:
		hour, minute, second = timestamp.split(':')
		start_dt 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), int(second))
	return start_dt, end_dt
	
if __name__ == '__main__':
	
	if len(sys.argv) != 3:
		print 'Usage:./search.py <TIMESTAMP|TIMESTAMP RANGE> <FILE>'
		print 'Example: ./search.py 8:42:04 log.dat'
		print '\t- Log lines with precise timestamp'
		print 'Example:./search.py 10:01 log.dat'
		print '\t- Log lines with timestamps between 10:01:00 and 10:01:59'
		print 'Example:./search.py 23:59-0:03 log.dat'
		print '\t- Log lines between 23:59:00 and 0:03:59'
	else:
		timestamp, filename = sys.argv[1], sys.argv[2]
		
		try:
			start_ts, end_ts = parse_timestamp(timestamp)
		except ValueError:
			raise
			print 'Invalid timestamp format'
		else:
			start_time = time.time()
			count = 0
			logs = RedditGrepClone(filename)
			for log in logs.find(start_ts, end_ts):
				print log.replace('\n', '')
				count += 1
			print '%d logs found in %f seconds.' % (count, time.time() - start_time)
