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

TODAY = datetime(2011,2,10)
START_OF_TODAY = datetime(TODAY.year, TODAY.month, TODAY.day, 0, 0, 0)
END_OF_TODAY = datetime(TODAY.year, TODAY.month, TODAY.day, 23, 59, 59)

class RedditGrepClone(object):
	
	def __init__(self, filename):
		self.file = open(filename, 'rb')
		self.file_size = os.path.getsize(filename)
		
	def find(self, abs_start_ts, abs_end_ts):
		'''
			Utilize binary search to find the logs matching
			the specified timestamps.
			@return generator of logs
		'''
		# Specific timestamp
		if abs_end_ts is None: abs_end_ts = abs_start_ts
		
		# First and last log timestamps
		first_ts = self._date_at_offset()
		self.file.seek(self.file_size - 2)
		last_ts = self._date_at_offset()
		last_offset = self.file.tell()
		self.file.seek(0)
		
		searches = [] # Set of timestamp ranges we need to search for. In the form of (start_ts, end_ts)
		offsets = [] # Set of offset ranges need to output
		
		# Handle all possible rollovers
		# 23:59:00-00:00:01
		# 23:59:50-23:59:55
		# 00:00:11-00:00:15
		# Don't even bother trying to figure out every little special
		# case. The binary search is so fast, doing one or two extra
		# isn't really that significant in this case
		one_day = timedelta(days = 1)
		if abs_start_ts > abs_end_ts:
			searches.append((abs_start_ts - one_day, abs_end_ts))
			searches.append((abs_start_ts, abs_end_ts + one_day))
		else:
			searches.append((abs_start_ts, abs_end_ts))
			searches.append((abs_start_ts + one_day, abs_end_ts + one_day))
			searches.append((abs_end_ts - one_day, abs_end_ts - one_day))
		
		for start_ts, end_ts in searches:
			start_offset, end_offset = None, None
			
			upper_bound = self.file_size
			lower_bound = 0
			prev_seek_offset = -1
			i = 0
			print '_______________'
			while i < 10:
				i += 1
				time.sleep(.5)
				seek_offset = ((upper_bound - lower_bound) / 2) + lower_bound
				print str(upper_bound) + ' ' + str(lower_bound) + ' ' + str(seek_offset)
				if prev_seek_offset == seek_offset:
					break
				else:
					prev_seek_offset = seek_offset
				self.file.seek(seek_offset)
				seek_ts = self._date_at_offset()
				print str(start_ts) + ' ' + str(seek_ts)
				if seek_ts > start_ts: # Passed it
					print 'Passed it'
					upper_bound = seek_offset
				elif seek_ts < start_ts: # Before it
					print 'Before it'
					lower_bound = seek_offset
				else:
					print 'Found one'
					upper_bound = seek_offset
					prev_seek_offset = -1
					j = 0
					while j < 10:
						j += 1
						time.sleep(.5)
						seek_offset = ((upper_bound - lower_bound) / 2) + lower_bound
						print str(upper_bound) + ' ' + str(lower_bound) + ' ' + str(seek_offset)
						self.file.seek(seek_offset)
						seek_ts = self._date_at_offset()
						if seek_ts == start_ts:
							self.file.seek(self.file.tell() - 1)
							prev_ts = self._date_at_offset()
							if prev_ts != start_ts:
								print 'Found first 1'
								self.file.seek(seek_offset)
								self._date_at_offset()
								start_offset = self.file.tell()
								print self.file.readline()
								break
							else:
								upper_bound = seek_offset
						else:
							self.file.readline()
							next_ts = self._date_at_offset()
							if next_ts == start_ts:
								print 'Found first 2'
								start_offset = self.file.tell()
								print self.file.readline()
								break
							else:
								lower_bound = seek_offset	
					break
			upper_bound = self.file_size
			if start_offset is None:
				lower_bound = 0
			else:
				lower_bound = start_offset
				
			prev_seek_offset = -1
			i = 0
			print 'XXXXXXXXXXXXXXXXXXXX'
			while i < 20:
				i += 1
				time.sleep(.5)
				seek_offset = ((upper_bound - lower_bound) / 2) + lower_bound
				print str(upper_bound) + ' ' + str(lower_bound) + ' ' + str(seek_offset)
				if prev_seek_offset == seek_offset:
					print 'Prev seek fault'
					break
				else:
					prev_seek_offset = seek_offset
				self.file.seek(seek_offset)
				seek_ts = self._date_at_offset()
				print str(start_ts) + ' ' + str(seek_ts)
				if seek_ts > end_ts: # Passed it
					print 'Passed it'
					upper_bound = seek_offset
				elif seek_ts < end_ts: # Before it
					print 'Before it'
					lower_bound = seek_offset
				else:
					lower_bound = seek_offset
					j = 0
					while j < 10:
						j += 1
						time.sleep(.5)
						seek_offset = ((upper_bound - lower_bound) / 2) + lower_bound
						print str(upper_bound) + ' ' + str(lower_bound) + ' ' + str(seek_offset)
						self.file.seek(seek_offset)
						seek_ts = self._date_at_offset()
						if seek_ts == end_ts:
							self.file.readline()
							next_ts = self._date_at_offset()
							if next_ts != end_ts:
								print 'Found last 1'
								self.file.seek(seek_offset)
								self._date_at_offset()
								print self.file.readline()
								break
							else:
								lower_bound = seek_offset
						else:
							self.file.seek(self.file.tell() - 1)
							prev_ts = self._date_at_offset()
							if prev_ts == end_ts:
								print 'Found last 2'
								print self.file.readline()
								break
							else:
								upper_bound = seek_offset
					break
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
			if char == '\n' and reads > 0: break
			reads += 1
			
		# Timestamp parts could be delimited by more than one space
		line = self.file.readline()
		fixed_line = re.sub('\s+', ' ', line, 3)  
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
	precice_ts = '[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'
	start, end = None, None
	
	if timestamp.find('-') > -1:
		start_ts, end_ts = timestamp.split('-')
	
		if re.match(precice_ts, start_ts) is None:
			hour, minute = timestamp.split(':')
			start = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute))
		else:
			hour, minute, second = timestamp.split(':')
			start = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), int(second))
	
		if re.match(precice_ts, end_ts) is None:
			hour, minute = timestamp.split(':')
			end 	= datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), 59)
		else:
			end = datetime(TODAY.year, TODAY.month, TODAY.day, int(hour), int(minute), int(second))
	elif re.match(precice_ts, timestamp) is None:
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
			start_time = time.time()
			count = 0
			logs = RedditLogFinder(filename)
			for log in logs.find(start_ts, end_ts):
				print log
				count += 1
			print '%d logs found in %f seconds.' % (count, time.time() - start_time)
