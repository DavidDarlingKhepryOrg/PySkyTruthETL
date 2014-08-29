# -*- coding: utf-8 -*-

#******************************************************************************
# Copyright 2014 Khepry Software
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#******************************************************************************
#
#******************************************************************************
# 
# This program will extract-transform-load (ETL) fracking data
# as obtained from FracFocus.org's website by SkyTruth.org.
#
# SkyTruth has published this extracted data in three file formats:
#     1. a tab-delimited "Reports" file
#     2. a tab-delimited "Chemicals" file
#     3. a tab-delimited "Blended" file ("Reports" + "Chemicals")
#
# This program pulls in the "Reports" and "Chemicals" files,
# blends them together, and outputs a "Bleneded" file.  To do so,
# the "python3-pandas" library is used to simplify the processing.
#
# The hyperlinks to the extracted files used within this program are as follows:
#    [2012 FracFocus Reports](https://s3.amazonaws.com/skytruth_public/2012_FracFocusReport.txt.zip)
#    [2012 FracFocus Chemicals](https://s3.amazonaws.com/skytruth_public/2012_FracFocusChemical.txt.zip)
#    [2013 FracFocus Reports](https://s3.amazonaws.com/skytruth_public/2013_FracFocusReport.txt.zip)
#    [2013 FracFocus Chemicals](https://s3.amazonaws.com/skytruth_public/2013_FracFocusChemical.txt.zip)
#
# This program was authored using:
#    1. Python 3.4.0 (64-bit)
#    2. Spyder 2.3.0 IDE (64-bit)
# running under:
#    3. Linux Mint 17 (64-bit), an alternative distro of Ubuntu 14.04.1 LTS
#
#******************************************************************************

import codecs
import gc
import mysql.connector
import os
import pandas
# import pyodbc # commented out because not yet available in Python 3
import sqlite3
import sys

# This path is for the source data files
dataFilePath = '~/Documents/fracking/SkyTruth'

# This path is for the target data files
trgtFldrPath = '~/Documents/fracking/tempFldr'
trgtFileDelim = ','
trgtBlndFile = 'blndFile.txt'
trgtChemFile = 'chemFile.txt'
trgtRprtFile = 'rprtFile.txt'
trgtSqliteDb = 'FracFocusXtract.db'
trgtDatabase = ''

# These file names are the various "Report" file names
rprtFileNames = ['2012_FracFocusReport.txt', '2013_FracFocusReport.txt']
rprtFileDelim = '\t'

# These file names are the various "Chemical" file names
chemFileNames = ['2012_FracFocusChemical.txt', '2013_FracFocusChemical.txt']
chemFileDelim = '\t'

# MySQL database connection values
sqlDriver = 'MySQL ODBC 5.3 Unicode Driver'
sqlHost = 'localhost'
sqlPort = '3306'
sqlDB = 'FracFocusXtract'
sqlUID = 'root'
sqlPWD = 'Radical1'

# ============================================================================
# Mainline Processing Routine
# ============================================================================

def main():
    
    # Verify that all specified
    # report file paths exist
    for rprtFileName in rprtFileNames:
        rprtFileNameExpanded = os.path.abspath(os.path.expanduser(os.path.join(dataFilePath, rprtFileName)))
        print ("Report file: %s%s" % (rprtFileNameExpanded, os.linesep))
        if not os.path.exists(rprtFileNameExpanded):
            sys.stderr.write("Report file does NOT exist: %s%s" % (rprtFileNameExpanded, os.linesep))
            return
            
    
    # Verify that all specified
    # chemical file paths exist
    for chemFileName in chemFileNames:
        chemFileNameExpanded = os.path.abspath(os.path.expanduser(os.path.join(dataFilePath, chemFileName)))
        print ("Chemical file: %s%s" % (chemFileNameExpanded, os.linesep))
        if not os.path.exists(chemFileNameExpanded):
            sys.stderr.write("Chemical file does NOT exist: %s%s" % (chemFileNameExpanded, os.linesep))
            return
        
    # If necessary, create the temporary folder
    trgtFldrPathExpanded = os.path.abspath(os.path.expanduser(trgtFldrPath))
    print ("Target folder: %s%s" % (trgtFldrPathExpanded, os.linesep))
    if not os.path.exists(trgtFldrPathExpanded):
        print ("Creating target folder: %s%s" % (trgtFldrPathExpanded, os.linesep))
        os.makedirs(trgtFldrPathExpanded)


    # Delete all resulting files prior to their re-creation
    # so that if the program abends, the end-user won't
    # get the impression that the program was successful
    # based upon the presence of all of the expected files.
    rprtFileExpanded = os.path.join(trgtFldrPathExpanded, trgtRprtFile)
    if os.path.exists(rprtFileExpanded):
        os.remove(rprtFileExpanded)
    chemFileExpanded = os.path.join(trgtFldrPathExpanded, trgtChemFile)
    if os.path.exists(chemFileExpanded):
        os.remove(chemFileExpanded)
    blndFileExpanded = os.path.join(trgtFldrPathExpanded, trgtBlndFile)
    if os.path.exists(blndFileExpanded):
        os.remove(blndFileExpanded)
    sqliteDbExpanded = os.path.join(trgtFldrPathExpanded, trgtSqliteDb)
    if os.path.exists(sqliteDbExpanded):
        os.remove(sqliteDbExpanded)
        
    # Append the report file(s) into one report file
    for fileName in rprtFileNames:
        fileNameExpanded = os.path.abspath(os.path.expanduser(os.path.join(dataFilePath, fileName)))
        print ("Report file: %s%s" % (fileNameExpanded, os.linesep))
        if not os.path.exists(fileNameExpanded):
            sys.stderr.write("Report file does NOT exist: %s%s" % (fileNameExpanded, os.linesep))
            return
        dataFrame = pandas.read_csv(fileNameExpanded, sep=rprtFileDelim, error_bad_lines=False)
        dataFrame = dataFrame.fillna(' ')
        with codecs.open(rprtFileExpanded, 'a') as f:
            dataFrame.to_csv(f, index=False)
        # clean up memory
        del dataFrame
        gc.collect()
        
    # Append the chemical file(s) into one chemical file
    for fileName in chemFileNames:
        fileNameExpanded = os.path.abspath(os.path.expanduser(os.path.join(dataFilePath, fileName)))
        print ("Chemical file: %s%s" % (fileNameExpanded, os.linesep))
        if not os.path.exists(fileNameExpanded):
            sys.stderr.write("Chemical file does NOT exist: %s%s" % (fileNameExpanded, os.linesep))
            return
        dataFrame = pandas.read_csv(fileNameExpanded, sep=chemFileDelim, error_bad_lines=False)
        dataFrame = dataFrame.fillna(' ')
        with codecs.open(chemFileExpanded, 'a') as f:
            dataFrame.to_csv(f, index=False)
        # clean up memory
        del dataFrame
        gc.collect()
            
    # Pull the appended report file into a DataFrame
    # NOTE: Be sure to use 'low_memory=False' parameter on read_csv,
    # otherwise it's possible that you won't get all of the rows
    with codecs.open(rprtFileExpanded, 'rb', 'cp1252') as rprt:
        rprtDataFrame = pandas.read_csv(rprt,
                                        low_memory=False,
                                        usecols=['pdf_seqid','r_seqid','api','fracture_date','state','county','operator','well_name','production_type','latitude','longitude','datum','true_vertical_depth','total_water_volume','published'])
        
    # Pull the temporary chemical file into a DataFrame
    # NOTE: Be sure to use 'low_memory=False' parameter on read_csv
    # otherwise it's possible that you won't get all of the rows
    with codecs.open(chemFileExpanded, 'rb', 'cp1252') as chem:
        chemDataFrame = pandas.read_csv(chem,
                                        low_memory=False,
                                        usecols=['pdf_seqid','c_seqid','row','trade_name','supplier','purpose','ingredients','cas_number','additive_concentration','hf_fluid_concentration','comments','cas_type'])
        
    # Merge the chemicals with the reports
    # and output the resulting "blended" file
    print ("Blending the report rows with the chemical rows")
    blndDataFrame = pandas.merge(rprtDataFrame, chemDataFrame, on='pdf_seqid', how='inner', suffixes=('_rpt','_chm'))
    blndDataFrame.fillna(' ')
    with codecs.open(blndFileExpanded, 'wb', 'cp1252') as f:
        blndDataFrame.to_csv(f, index=False)
        # Uncomment the "pandas.merge" statement below if you want to pull in the data,
        # blend it, and push it out to the "blended" file without creating an "blended" DataFrame in between.
        # If you do so, remember to comment out the "blndDataFrame" statements above and below as they're aren't needed.
        # pandas.merge(rprtDataFrame, chemDataFrame, on='pdf_seqid', how='inner', suffixes=('_rpt','_chm')).to_csv(f, index=False)

    # Output the resulting DataFrames
    # to the specified database/tables

    print ("Output the results to an SQLite database")
    
    # cnx = sqlite3.connect(':memory:')
    cnx = sqlite3.connect(sqliteDbExpanded)
   
    rprtDataFrame.to_sql('reports', cnx, flavor='sqlite', if_exists='replace')
    chemDataFrame.to_sql('chemicals', cnx, flavor='sqlite', if_exists='replace')
    blndDataFrame.to_sql('reports_chemicals', cnx, flavor='sqlite', if_exists='replace')

    cnx.close()
    del cnx
    
    print ("Establish a connection to the MySQL database")        

    modality = None
    try:
        # attempt connection with ODBC
        sqlConnString = "Driver={%s};Server=%s;Port=%s;UID=%s;Database=%s;" % (sqlDriver, sqlHost, sqlPort, sqlUID, sqlDB)
        print ("pyODBC srcConnString: %s" % sqlConnString)
        sqlConnString += "PWD=%s;" % sqlPWD
        sqlConnection = pyodbc.connect(sqlConnString)
        modality = "pyodbc"
    except:
        # attempt connection with MySQL connector
        sqlConnParms = {'user':sqlUID, 'host':sqlHost, 'port':sqlPort, 'database':sqlDB}
        print ("MySQL connString: %s" % sqlConnParms)
        sqlConnParms['password'] = sqlPWD
        sqlConnection = mysql.connector.connect(**sqlConnParms)
        modality = "mysql.connector"

#    print ("Output the results to MySQL database")        
#
#    rprtDataFrame.to_sql('reports', sqlConnection, flavor='mysql', if_exists='replace')
#    chemDataFrame.to_sql('chemicals', sqlConnection, flavor='mysql', if_exists='replace')
#    blndDataFrame.to_sql('reports_chemicals', sqlConnection, flavor='mysql', if_exists='replace')
    
    sqlConnection.close()
    del sqlConnection
        
    # Clean up memory
    del blndDataFrame
    del chemDataFrame
    del rprtDataFrame
    gc.collect()

    return

    
# ============================================================================
# Execute the Mainline Processing Routine
# ============================================================================
    
if (__name__ == "__main__"):
    retval = main()
