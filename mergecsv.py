#!/usr/bin/env python
# encoding: utf-8
"""
mergecsv.py

This script will merge several csv files into one.

Created by spurge on 2012-03-25.
Copyright (c) 2012 Klandestino AB. All rights reserved.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
"""
import sys
import os
import getopt
import csv
import sqlite3

help_message = '''
\033[1;32mmergecsv.py \033[0;33m[options] csv files ...\033[0m merges specified csv files into one.

Available options are:

  \033[0;33m-o\033[0m | \033[0;33m--output\033[0m    Specifies where to write the merged data.
                   If not present, the merged data will be
                   printed in your console.

  \033[0;33m-v\033[0m               Verbose. Prints almost everything that
                   happens. Only works with -o (or --output)
                   specified.

  \033[0;33m-h\033[0m | \033[0;33m--help\033[0m      Prints this help-text.
'''

class Usage( Exception ):
	def __init__( self, msg ):
		self.msg = msg

def main( argv = None ):
	if argv is None:
		argv = sys.argv

	try:
		try:
			opts, args = getopt.getopt( argv[ 1: ], "ho:v", [ "help", "output=" ] )
		except getopt.error, msg:
			raise Usage( msg )

		verbose = False
		output = None

		for option, value in opts:
			if option == "-v":
				verbose = True
			if option in ( "-h", "--help" ):
				raise Usage( help_message )
			if option in ( "-o", "--output" ):
				output = value

		try:
			readers = []
			for file in args:
				readers.append( {
					'file': file,
					'data': csv.reader( open( file, 'rb' ) ),
					'cols': []
				} )
		except IOError, msg:
			raise Usage( msg )
	
		if not output is None:
			try:
				writer = csv.writer( open( output, 'wb' ) )
			except IOError, msg:
				raise Usage( msg )
		else:
			writer = csv.writer( sys.stdout )

		# Kollar liknande kolumner för matchning
		cols = {}
		for reader in readers:
			header = reader[ 'data' ].next()
			for col in header:
				reader[ 'cols' ].append( col )
				if col in cols:
					cols[ col ] += 1
				else:
					cols[ col ] = 0
			if verbose and not output is None:
				print >> sys.stdout, 'Found {0} columns \033[0;32m{1}\033[0m in \033[0;33m{2}\033[0m'.format(
					len( reader[ 'cols' ] ),
					reader[ 'cols' ],
					reader[ 'file' ]
				)

		cols = sorted( cols.iteritems(), reverse = True, key = lambda( k, v ): [ v, k ] )

		if verbose and not output is None:
			print >> sys.stdout, 'Merged columns are \033[0;32m{0}\033[0m'.format( cols )

		try:
			dbcon = sqlite3.Connection( ':memory:' )
			dbcur = dbcon.cursor()

			sql = 'CREATE TABLE "csvmerge" ('

			for col in cols:
				sql += '"{0}" varchar(128),'.format( col[ 0 ] )

			sql = sql[ 0:-1 ] + ')'
			dbcur.execute( 'DROP TABLE IF EXISTS csvmerge' )
			dbcur.execute( sql )
		except sqlite3.OperationalError, msg:
			raise Usage( msg )

		rowcount = 0
		mergecount = 0

		for reader in readers:
			for row in reader[ 'data' ]:
				sqlcol = ''
				sqldata = ''
				sqlupdate = ''
				colmatch = []
				hasmatched = False
				i = 0

				for data in row:
					sqlcol += reader[ 'cols' ][ i ] + ','
					sqldata += '"{0}",'.format( data )
					sqlupdate += '{0}="{1}",'.format( reader[ 'cols' ][ i ], data )
					for col in cols:
						if col[ 0 ] == reader[ 'cols' ][ i ] and col[ 1 ] > 0:
							colmatch.append( [ col[ 0 ], data ] )
							break
					i += 1

				if len( colmatch ):
					sqlmatch = ''
					for colname, colval in colmatch:
						sqlmatch += '{0}="{1}" AND '.format( colname, colval )
					sqlmatch = sqlmatch[ 0:-5 ]
					matchresult = dbcur.execute( 'SELECT COUNT(*) FROM csvmerge WHERE ' + sqlmatch )
					matchrow = matchresult.fetchone()
					if matchrow[ 0 ] > 0:
						hasmatched = True

				if hasmatched:
					dbcur.execute( 'UPDATE csvmerge SET {0} WHERE {1}'.format( sqlupdate[ 0:-1 ], sqlmatch ) )
					mergecount += 1
				else:
					dbcur.execute( 'INSERT INTO csvmerge ({0}) VALUES ({1})'.format( sqlcol[ 0:-1 ], sqldata[ 0:-1 ] ) )
					rowcount += 1

		if verbose and not output is None:
			print >> sys.stdout, '\033[0;32m{0}\033[0m new rows was found and inserted. \033[0;33m{1}\033[0m rows was merged.'.format( rowcount, mergecount )

		result = dbcur.execute( 'SELECT * FROM csvmerge' )
		resultcount = 0
		header = []

		for col in result.description:
			header.append( col[ 0 ] )
		writer.writerow( header )

		for row in result:
			data = []
			for col in row:
				if col is None:
					data.append( '' )
				else:
					data.append( col )
			writer.writerow( data )
			resultcount += 1

		if verbose and not output is None:
			print >> sys.stdout, '\033[0;32m{0}\033[0m rows of data was dumped \033[0;33m{1}\033[0m'.format( resultcount, output )

		dbcur.close();
		dbcon.commit();
		dbcon.close();

	except Usage, err:
		print >> sys.stderr, str( err.msg )
		print >> sys.stderr, "For help use --help"
		return 2

if __name__ == "__main__":
	sys.exit( main() )
