import os, os.path, sys
from datetime import datetime
from daettime import timedelta

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
TODAY = datetime.today()

class RedditLogSearcher(object):
	
	def __init__(self, filename):
		self.file = open(filename, 'rb')
		self.file_size = os.path.getsize(filename)
		
	def search(self, start_ts, end_ts):
		'''
			Utilize binary search to find the logs matching
			the specified timestamps.
			@return generator of logs
		'''
		start_offset, end_offset = None, None
		
		# First and last log timestamps
		first_ts = self._date_at_offset()
		self.file.seek(self.file_size)
		last_ts = self._date_at_offset()
		last_offset = self.file.tell()
		self.file.seek(0)
		
		# Day rollovers 
		if first_ts < TODAY and start_ts.hour > end_ts.hour:
			# If the rollover is at the beginning of the file
			# and input is something like 23:59-0:3:10.
			start_ts = start_ts - timedelta(days = 1)
		if last_ts > TODAY and start_ts.hour > end_ts.hour:
			# If the rollover is at the end of the file
			# and input is something like 23:59-0:3:10
			end_ts = end_ts + timedelta(days = 1)
		
		# Special cases
		if end_ts is not None and first_ts > end_ts:
			return
		if start_ts > last_ts:
			return
		
		# Find a log that matches start timestamp
		lower_bound = 0
		upper_bound = self.file_size
		while True:
			if upper_bound == lower_bound:
				if end_ts is not None:
					# Closest without going over
					start_offset = self.file.tell()
				else:
					return
			else:
				seek_offset = (upper_bound - lower_bound) / 2
				self.file.seek(seek_offset)
				seek_ts = self._date_at_offset()
				if start_ts == seek_ts:
					# Find the first log in a list of logs
					# with the same timestamp
					upper_bound = seek_offset
					while True:
						seek_offset = (upper_bound - lower_bound) / 2
						if seek_offset == 0:
							start_offset = 0
							break
						self.file.seek(seek_offset)
						seek_ts = self._date_at_offset()
						if start_ts == seek_ts:
							self.file.seek(seek_to - 1)
							prev_ts = self._date_at_offset()
							if start_ts != prev_ts:
								start_offset = seek_offset
								break
							else:
								upper_bound = seek_offset
						else:
							self.file.readline()
							next_ts = self._date_at_offset()
							if start_ts == next_ts:
								start_offset = seek_offset
								break
							else:
								lower_bound = seek_offset
					break
				elif start_ts > seek_ts: # Later
					upper_bound = seek_offset
				elif start_ts < seeked_ts: # Before
					lower_bound = seek_ts
		if end_ts is None:
			end_ts = start_ts
		# Find a log that matches the end timetamp
		lower_bound = start_offset
		uppder_bound = self.file_size
		while True:
			if upper_bound == lower_bound:
				end_offset = self.file.tell()
			else:
				seek_offset = (upper_bound - lower_bound) / 2
				self.file.seek(seek_offset)
				seek_ts = self._date_at_offset()
				if end_ts == seek_ts:
					# Find the last log in a list of logs
					# with the same timestamp
					lower_bound = seek_offset
					while True:
						seek_offset = (upper_bound - lower_bound) / 2
						self.file.seek(seek_offset)
						if seek_offset == last_offset:
							end_offset = seek_offset
							break
						seek_ts = self._date_at_offset()
						if end_ts == seek_ts:
							self.file.readline()
							next_ts = self._date_at_offset()
							if end_ts != next_ts:
								end_offset = seek_offset
								break
							else:
								lower_bound = seek_offset
						else:
							self.file.seek(seek_offset - 1)
							prev_ts = self._date_at_offset()
							if end_ts == prev_ts:
								end_offset = seek_offset
							else:
								upper_bound = seek_offset
					break
				elif end_ts > seek_ts: # Later
					upper_bound = seek_offset
				elif end_ts < seek_ts: # Before
					lower_bound = seek_offset
		self.file.seek(start_offset)
		while self.file.tell() <= self.end_offset:
			yield self.file.readline()
			
	def _date_at_offset(self):
		'''
			Backtrack to find the beginning of the current line and parses
			timestamp. File cursor will be moved to the beginning of the line.
			@return datetime of the log at current file offset
		'''
		
		start_offset, reads, seek_to = self.file.tell(), 0, None
		while True:
			seek_to = start_offset - reads
			self.file.seek(seek_to)
			if seek_to == 0: break # beginning of file
			char = self.file.read(1)
			if (char == '\n' or char == '\r') and reads > 0: break
			reads += 1
			
		# Timestamp parts could be delimited by more than one space
		fixed_line = re.sub('\s+', ' ', self.file.readline(), 3)  
		month, day, timestamp, log = fixed_line.split(' ', 3)
		hour, minute, second = timestamp.split(':')
		
		# readline() moved the cursor to end of the line, move it back
		# to the beginning
		self.file.seek(seek_to)
		
		return datetime(TODAY.year, MONTH_MAP[month.lower()], int(day), int(hour), int(minute), int(second))

def parse_timestamp(timestamp):
	assert isinstance(timestamp,types.StringType)
	
	# Any possible arguement must match this regular expression
	valid_arg = '[0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2})?(-[0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2})?)?'
	if re.match(valid_stamp_regex, timestamp) is None:
		raise ValueError
	
	# Any timestamp matching this regular expresssion is not a wildcard
	precice_ts = '[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'
	start, end = None, None
	
	if timestamp.find('-') > -1:
		start_ts, end_ts = timestamp.split('-')
	
		if re.match(precise_stamp_regex, start_ts) is None:
			hour, minute = timestamp.split(':')
			start = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute))
		else:
			hour, minute, second = timestamp.split(':')
			start = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), int(second))
	
		if re.match(end_ts, precise_stamp_regex) is None:
			hour, minute = timestamp.split(':')
			end 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), 59)
		else:
			end = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), int(second))
	elif re.match(precise_stamp_regex, timestamp) is None:
		hour, minute = timestamp.split(':')
		start 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute))
		end 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), 59)
	else:
		hour, minute, second = timestamp.split(':')
		start 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), int(second))
	return start, end
	
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
			print 'Invalid timestamp format'
		else:
			try:
				reddit_logs = RedditLogFile(filename)
			except IOError:
				print 'Unable to open log file'
			else:
				try:
					for log in reddit_logs.search(start_ts, end_ts):
						print log
				except RedditLogFile.SearchingError, e:
					print 'Error: ' + str(e)
	
