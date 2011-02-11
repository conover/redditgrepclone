#! /usr/bin/python
import sys, os, re, types, os.path
from datetime import datetime

# Special Cases
## End of year rollover
## Multiple logs occurring in the same second
## Log does exist at specified time stamp

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


def line_at_offset(file, offset = None):
	'''
		Backtracks to find the beginning of the current line.
		Returns line initial offset and the line itself.
		File offset will be set to the first character after the line.
	'''
	if offset is None:
		offset = file.tell()

	line_start, line_end = None, None

	# Seek back until we find the beginning of the line
	reads = 0
	while True:
		target_offset = offset - reads
		file.seek(target_offset)
		if target_offset == 0: break
		char = file.read(1)
		if (char == '\n' or char == '\r') and reads > 0: break
		reads += 1
	return file.tell(), file.readline()
	
def date_from_line(line):
	'''
		Parse date from line. Expects Reddit log format
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
				file = open(filename, 'rb', 0)
			except IOError:
				print 'Unable to open log file'
			else:
				
				first_log_offset = None
				bounding = os.path.getsize(filename)
				
				# Find the start offset
				while True:
					current_offset = file.tell()
					line_offset, line = line_at_offset(file)
					line_ts = date_from_line(line)

					if bounding == 1:
						break

					bounding = int(bounding / 2)
					print 'Bounding: ' + str(bounding) + ' ' + str(start_ts) + ' ' + str(line_ts)
					if start_ts > line_ts: # We passed it
						
						file.seek(current_offset + bounding)
					elif start_ts < line_ts: # We haven't reached it yet
						file.seek(current_offset - bounding)
					
				current_line_offset = line_offset
				current_line_ts = line_ts
				while True:
					prev_line_offset, prev_line = line_at_offset(file, current_line_offset - 1)
					prev_line_ts = date_from_line(prev_line)
				
					if prev_line_ts != current_line_ts:
						print prev_line
						first_log_offset = current_line_offset
						break
					current_line_offset = prev_line_offset
					current_line_ts = prev_line_ts
				print line_at_offset(file, first_log_offset)
				