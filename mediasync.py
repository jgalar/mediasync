#! /usr/bin/env python3
#
# Script meant to take media files from a generic source (e.g. a torrent destination
# directory) and copy the media files to a set of predefined directories to allow editing
# without altering the original files following a couple of criterions in the .mediasync config file.
# Maintains a simple "media sync database" which is a list of files already processed by the script.
# 
# © 2012 Jérémie Galarneau

import sys
import os
import argparse
import configparser
import re
import sqlite3
from datetime import datetime
import shutil
from optparse import OptionParser

class CopyCommand:
    sourcePath = None
    destinationPath = None
    
    def __init__( self, sourceRoot, destinationRoot, sourcePath ):
        # Mash things up
        if re.search( re.escape(sourceRoot), sourcePath ) is None:
            print( "sourceRoot: " + sourceRoot )
            print( "destinationRoot: " + destinationRoot )
            print( "sourcePath: " + sourcePath )
            raise AssertionError( "sourcePath does not include sourceRoot" )
        
        # TODO Check for trailing /
        self.sourcePath = sourcePath
        self.destinationPath = destinationRoot + sourcePath.replace( sourceRoot, "" )

    def run( self ):
        # Run command and return boolean success value
        #print( "Copy operation: " + self.sourcePath + " -> " + self.destinationPath )
        
        # Check and create necessary directory structure
        lastSlashIndex = self.destinationPath.rfind( "/" )
        destinationFolder = self.destinationPath[:lastSlashIndex]
        if not os.path.isdir( destinationFolder ):
            try:
                os.makedirs(destinationFolder)
            except OSError as osErrorException:
                sys.stderr.write( str(osErrorException) + "\n" )
                return False
        
        # Copy that floppy
        try:
            shutil.copyfile( self.sourcePath, self.destinationPath )
        except IOError as ioErrorException:
            sys.stderr.write( str(ioErrorException) + "\n" )
            return False

        return True

class MediaCategory:
    name = None
    extensions = []
    destination = None
    exclusionRegex = None

    def __init__( self, name, extensions, destination, exclusionRegex ):
        self.name = name
        self.extensions = extensions
        self.destination = destination
        self.exclusionRegex = exclusionRegex

    def belongs( self, path ):
        extensionFound = ( extensions == None )
        for extension in self.extensions:
            if re.search( re.escape(extension) + "$", path, re.IGNORECASE ) is not None:
                extensionFound = True
                break

        if not extensionFound:
            return False

        if self.exclusionRegex is not None:
            return self.exclusionRegex.search( path ) is None
        else:
            return True


# Open/Create the MediaSync database and return a connection
def openDB(path):
    dbConnection = sqlite3.connect(path)
    
    # Initialize schema (time stored as ISO8601 strings)
    SQLCreateTableString = "CREATE TABLE IF NOT EXISTS MediaSourceFiles(\
sourcePath TEXT,\
categoryName TEXT,\
destinationPath TEXT,\
processedTime TEXT )"
    dbConnection.execute( SQLCreateTableString )
    dbConnection.commit()
    return dbConnection

def getTimeString():
    return datetime.now().isoformat()

# ************************************************************************************************
configFilePath = os.path.expanduser( "~/.mediasync" )
SQLFindQueryTemplate = "SELECT EXISTS(SELECT 1 FROM MediaSourceFiles WHERE sourcePath=? AND categoryName=? LIMIT 1)"
SQLInsertQueryTemplate = "INSERT INTO MediaSourceFiles VALUES (?,?,?,?)"

if ( not os.path.exists( configFilePath ) ):
    sys.stderr.write( "Failed to open configuration file at path \"" + configFilePath + "\"\n" )
    sys.exit(-1)

configParser = configparser.SafeConfigParser()
configParser.read( os.path.expanduser("~/.mediasync") )

# Load required config infos
syncDBPath = None
mediaSourcePaths = None

try:
    syncDBPath = os.path.expanduser( configParser.get( "media_database", "db_path" ) )
    mediaSourcePaths = os.path.expanduser( configParser.get( "media_sources", "source_paths" ) )
    mediaSourcePaths = mediaSourcePaths.split( "," )
except configparser.NoOptionError as missingOptionException:
    sys.stderr.write( "Required option missing: " + str(missingOptionException) + "\n" )
    sys.exit(-1)

if ( not syncDBPath ):
    sys.stderr.write( "No sync database path specified.\n" )
    sys.exit(-1)

if ( not mediaSourcePaths ):
    sys.stderr.write( "No media source path(s) specified.\n" )
    sys.exit(-1)

mediaCategoryCount = len( configParser.sections() ) - 2
if ( not mediaCategoryCount ):
    print( "No media categorie(s) defined; nothing to do." )
    sys.exit(0)

optionParser = OptionParser()
optionParser.add_option( "-s", "--source", dest="explicitMediaPath", help="folder to scan for media", metavar="PATH" )
( options, args ) = optionParser.parse_args()

if ( options.explicitMediaPath is not None ):
    mediaSourcePaths = [ options.explicitMediaPath ]

database = openDB(syncDBPath)

# Init categories
mediaCategories = []
for section in configParser.sections():
    # Skip reserved section names
    reservedSectionNameRE = re.compile( r'^media', re.IGNORECASE )
    if len( reservedSectionNameRE.findall( section ) ) != 0 :
        continue

    extensions = []
    destination = None
    exclusionRegEx = None

    # No specified extensions = allow all
    if configParser.has_option( section, "extensions" ):
        extensions = configParser[section]["extensions"].split( ',' )

    try:
        destination = os.path.expanduser( configParser.get( section, "destination" ) )
    except configparser.NoOptionError as missingOptionException:
        sys.stderr.write( "Required option missing: " , str(missingOptionException) + "\n" )
        sys.exit(-1)
    
    if configParser.has_option( section, "exclude" ):
        exclusionRegEx = re.compile( configParser[section]["exclude"] )

    mediaCategories.append( MediaCategory( section, extensions, destination, exclusionRegEx ) )


# Process all files in the media source folders
for mediaSourcePath in mediaSourcePaths:
    for (root, dirs, files) in os.walk( mediaSourcePath ):
        for file in files:
            mediaFilePath = os.path.join( root, file )

            # Check if the entries exist in the MediaSync database, if not, submit to categories.
            for category in mediaCategories:
                activeCursor = database.cursor()
                activeCursor.execute( SQLFindQueryTemplate,( mediaFilePath, category.name ) )

                if activeCursor.fetchone()[0] == 1:
                    #print( "Found " + mediaFilePath  )
                    continue

                if not category.belongs( mediaFilePath ):
                    #print( mediaFilePath + " does not belong in " + category.name )
                    continue
        
                copyCommand = CopyCommand( mediaSourcePath, category.destination, mediaFilePath )
                if not copyCommand.run():
                    continue

                # Add entry to media sync database
                values = ( mediaFilePath, category.name, copyCommand.destinationPath, getTimeString() )
                print( "New media entry: " + str(values) )
                activeCursor.execute( SQLInsertQueryTemplate, values )

database.commit()
