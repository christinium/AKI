#!/usr/bin/python2.7
#
#
#from __future__ import print_function

import psycopg2
import psycopg2.extras
import sys
import re
import datetime
import numpy as np
import pandas as pd
import os
import datetime as dt

# use config object to import .ini files
from configobj import ConfigObj

###################################################
# define path variables used throughout the script
###################################################

# folder path of current file
base_path = os.sep.join(os.path.realpath(__file__).split(os.sep)[0:-1])

# get local path of repository by removing last folder from above
base_path = os.sep.join(base_path.split(os.sep)[0:-1])

## dictionary path with all the dictionary text files
#path = os.path.join(base_path,'dictionary2')

## output path for parsed echos
#output_path = os.path.join(base_path, 'CSV')

# db config file which has connection settings for MIMIC in a database
db_config_file = os.path.join(base_path,'python_scripts','db-config.ini')

###################################################
# SUBROUTINES
###################################################


## cuz I like perl
class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value
##



###################################################
#try to connect to database using a config file
###################################################

# check if we have a db-config
if os.path.isfile(db_config_file):
    config = ConfigObj(db_config_file)
    sqluser = config['username']
    sqlpass = config['password']
    sqlhost = config['host']
    sqlport = config['port']
    sqldb = config['dbname']
    sqlschema = config['schema_name']
else:

    raise ImportError('A db-config.ini file is required to connect to the database. See db-config.ini.example for an example.')

conn = psycopg2.connect(dbname=sqldb, host=sqlhost, port=sqlport,
                        user=sqluser, password=sqlpass)


cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
try:
    print("{} - Loading MIMIC-III notes into cursor.".format(dt.datetime.now()))
    cur.execute("""
SET search_path TO mimiciii;
select icustay_detail.*, KDIGO_CREAT.admcreat, KDIGO_CREAT.admcreattime, KDIGO_CREAT.highcreat48hr, KDIGO_CREAT.highcreattime48hr, KDIGO_CREAT.highcreat7day, KDIGO_CREAT.highcreattime7day, rrt.rrt,oasis.oasis
from icustay_detail
inner join
KDIGO_CREAT 
on KDIGO_CREAT.icustay_id = icustay_detail.icustay_id
join
rrt
on KDIGO_CREAT.icustay_id = rrt.icustay_id
join
oasis
on oasis.icustay_id = icustay_detail.icustay_id
where icustay_detail.age >=18
    """)
except:
    print("I can't SELECT from noteevents")
    raise
print("Fetching notes.")
rows = cur.fetchall()



aki_stage = AutoVivification()
num_each_stage = []
for i in range(5):
       num_each_stage.append(0)
for row in rows:
	if (row['admcreat'] is None):
		aki_stage[row['icustay_id']] = -1
		num_each_stage[0] = num_each_stage[0] + 1
	else:
		three_times_admit = 3* float(str(row['admcreat']))
		one_five_times_admit = 1.5*float(str(row['admcreat']))
		if ((three_times_admit<= row['highcreat7day']) or (row['highcreat7day']>=4)):
			aki_stage[row['icustay_id']] = 3
			num_each_stage[3] = num_each_stage[3] + 1
		elif (row['highcreat7day'] >= (2*row['admcreat'])):
			aki_stage[row['icustay_id']] = 2
			num_each_stage[2] = num_each_stage[2] + 1

		elif ((row['highcreat7day'] >= (one_five_times_admit)) or (row['highcreat48hr'] >= (0.3+row['admcreat'])) or row['rrt']==1):
			aki_stage[row['icustay_id']] = 1
			num_each_stage[1] = num_each_stage[1] + 1			
		else:
			num_each_stage[4] = num_each_stage[4] + 1
	#print type(row['rrt'])
print(num_each_stage[0])
print(num_each_stage[1])
print(num_each_stage[2])
print(num_each_stage[3])
print(num_each_stage[4])


# get table with in hospital death flag, 
#in hospital death flag
#death - time
#discharge time
#dialysis!!

