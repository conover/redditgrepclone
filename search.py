#! /usr/bin/python
import sys, os, re, types, os.path
from datetime import datetime

# Special Cases
## End of year rollover
## Multiple logs occurring in the same second
## Log does exist at specified time stamp
## Precise log does not exist but range

FILE_SIZE 	= 0
TODAY 		= datetime.today()
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


class RedditLogFile(object):
	
	def __init__(self, filename):
		self.file = open(filename, 'rb', 0)
		self.file_size = os.path.getsize(filename)
		
	def search(self, start_ts, end_ts):
		
		duration = False if end is None else True
		
		# Check some special cases
		if start >= end_ts:
			raise ValueError, 'Starting timestamp must be less than ending timestamp' 
		last_offset, last_ts = self._date_at_offset(self.file_size - 1)
		if start_ts > last_ts:
			# Last log's timestamp is less than specified start
			return ()
		if duration:
			first_offset, first_ts = self._date_at_offset(0)
			if end_ts < first_ts:
				# First log's timestamp is greater than specified end
				return ()

		start_offset, end_offset = None, None
		
		# Find the start log offset
		self.file.fseek(0)
		fence = self.file_size
		while True:
			current_offset, current_ts = self._date_at_offset()

			fence = int(fence / 2)
			#print 'Bounding: ' + str(bounding) + ' ' + str(start_ts) + ' ' + str(line_ts)
			if start_ts > current_ts: # Passed it	
				file.seek(current_offset + fence)
			elif start_ts < current_ts: # Haven't reached it yet
				file.seek(current_offset - fence)
			elif start_ts == current_ts:
				# Scan backward to make sure we have the
				# first log of this timestamp
				while current_offset > 0:
					prev_offset, prev_ts = self._date_at_offset(current_offset - 1)
					if current_ts != prev_ts:
						break
					else:
						current_offset = prev_offset
						current_ts = prev_ts
				break
			elif fence == 1 and duration:
				# Closest we are going to get for range
				break
			elif fence == 1:
				# No precise match as requested
				return ()
				
		start_offset = self.file.tell()
		
		if duration:
			# Find the end log offset
			fence = self.file_size - start_log_offset # End log must be past start log
			while True:
				current_offset, current_ts = self._date_at_offset()
				
				fence = int(fence / 2)
				#print 'Bounding: ' + str(bounding) + ' ' + str(start_ts) + ' ' + str(line_ts)
				if end_ts > current_ts: # Passed it	
					file.seek(current_offset - fence)
				elif end_ts < current_ts: # Haven't reached it yet
					file.seek(current_offset +fence)
				elif end_ts == current_ts:
					# Scan forward to make sure we have the
					# last log of this timestamp
					while current_offset != last_offset:
						self.file.readline()
						next_offset, next_ts = self._date_at_offset()
						if current_ts != next_ts:
							break
						else:
							current_offset = next_offset
							current_ts = next_ts
					end_offset = current_ts
					break
				elif fence == 1:
					end_offset = self.file_size
					break
					
		self.file.fseek(start_offset)
		while end_offset > self.file.tell():
			yield self.file.readline()
				
	def _date_at_offset(self, offset = None):
		'''
			Backtracks to find the beginning of the current line.
			Returns line initial offset and the date of the line.
			File offset will be set to the first character after the line.
		'''
		if offset is None:
			offset = self.file.tell()
	
		line_start, line_end = None, None
	
		# Seek back until we find the beginning of the line
		reads = 0
		while True:
			target_offset = offset - reads
			self.file.seek(target_offset)
			if target_offset == 0: break
			char = self.file.read(1)
			if (char == '\n' or char == '\r') and reads > 0: break
			reads += 1
		return self.file.tell(), self._date_from_line(self.file.readline())
		
	def _date_from_line(line):
		'''
			Parse date from line. Expects Reddit log format.
			Returns datetime
		'''
		fixed_line = re.sub('\s+', ' ', line, 3)
		month, day, timestamp, log = fixed_line.split(' ', 3)
	
		hour, minute, second = timestamp.split(':')
	
		return datetime(TODAY.year, MONTH_MAP[month.lower()], int(day), int(hour), int(minute), int(second))
			
def parse_timestamp(timestamp):
	assert isinstance(timestamp,types.StringType)
	
	valid_stamp_regex = '[0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2})?(-[0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2})?)?'
	if re.match(valid_stamp_regex, timestamp) is None:
		raise ValueError
	
	precise_stamp_regex = '[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'
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
		range = True
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
				for log in reddit_logs.search(start_ts, end_ts):
					print log