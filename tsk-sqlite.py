#!/usr/bin/env python

import sys
import getopt, sqlite3

class ParseMactime():
	def __init__(self, verboseFlag):
		self.db_name    = "timeline.db"
		self.conn       = None
		self.cursor     = None
		self.verbose 	= verboseFlag

	def connect_to_db(self):
		self.conn   = sqlite3.connect(self.db_name)
		self.cursor = self.conn.cursor()
		self.conn.text_factory = str
	
	def clear_tables(self):
	# Clean all tables
		try:
			self.cursor.execute("DROP TABLE tl")
		except sqlite3.OperationalError:
			print "Warning >>> Nothing to clean."
		# Date,Size,Type,Mode,UID,GID,Meta,File Name
		self.cursor.execute("""CREATE TABLE tl (
			datum TEXT,
			size INT,
			type TEXT,
			mode TEXT,
			uid TEXT,
			gid TEXT,
			meta TEXT,
			file TEXT)
			""")

		self.conn.commit()
		print "Info >>> Initalized table(s)"

	def parse_csv(self, fileName):
		try:
			f = open(fileName)
		except Exception:
			print ">>> Cannot open file %s" % fileName
			sys.exit(1)

		for l in f:
			l = l.strip()
			if len(l) <= 0:
				continue
			sl = l.split(',')

			# Comma delimited mactime file has always 8 entries.  If we detect
			# more than 8, we have commas in the filename
			if len(sl) > 8:
				sl[7] = "".join(sl[7:])
				if self.verbose:
					print ">>> Found bogus entry with length %d, shorten it to 8: %s" % (len(sl), sl[7])

			try:
				date    = sl[0]
				size    = int(sl[1])
				ftype   = sl[2]
				mode    = sl[3]
				uid 	= sl[4]
				gid 	= sl[5]
				meta    = sl[6]
				fname 	= sl[7]
			except IndexError as e:
				print ">>> Parsing error: %s" % str(e)
				continue

			query   = "INSERT INTO tl VALUES (?,?,?,?,?,?,?,?)"
			values  = (date,size,ftype,mode,uid,gid,meta,fname)
			self.cursor.execute(query, values)
			
		self.conn.commit()
		f.close()

def usage(msg = ""):
	print "Usage"
	print
	print " --initdb             - Initialize SQLite database (destroys all data!)"
	print " --import <csv-file>  - Import timeline from CSV file (comma separated)"
	print
	print " -v                   - Be more verbose"
	print
	print "%s" % msg
	sys.exit(1)

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'hv',
			["import=", "initdb"])
	except getopt.GetoptError as err:
		usage(str(err))

	if len(opts) == 0:
		usage("")

	importCsv  	= ""
	importCsvF 	= 0
	initDB 		= 0
	verboseFlag = 0

	for opt, arg in opts:
		if opt == '-h':
			usage("")
		elif opt == '--import':
			importCsv = arg
			importCsvF = 1
		elif opt == '--initdb':
			initDB = 1
		elif opt == '-v':
			verboseFlag = 1
		else:
			usage("Unrecognized Option")

	p = ParseMactime(verboseFlag)
	p.connect_to_db()

	if initDB > 0:
		p.clear_tables()
	elif importCsvF > 0:
		p.parse_csv(importCsv)

main()
