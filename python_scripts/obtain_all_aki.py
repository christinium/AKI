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

## output path for csv
output_path = os.path.join(base_path, 'output\\')

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
with order_icu as (
    select icu.*
        ,row_number() over(partition by icu.subject_id order by icu.admittime) as rn
    from mimiciii.icustay_detail as icu
    order by subject_id
),
first_icu as (
select * 
from order_icu
where rn = 1
),
serv as(
select ie.hadm_id, curr_service as first_service
    , ROW_NUMBER() over (partition by ie.hadm_id order by transfertime DESC) as rn
  from mimiciii.icustays ie
  inner join mimiciii.services se
    on ie.hadm_id = se.hadm_id
    and se.transfertime < ie.intime + interval '1' day
),
first_serv as (
select * 
from serv
where rn = 1

),
esrd_icd as(
select  hadm_id,
	max(case
        when icd9_code like '5856' then 1 -- ESRD
        else 0 end) as ESRD

from diagnoses_icd 
group by hadm_id
),
all_dis_creats as (
select
ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
, le.VALUENUM as DISCREAT
, le.CHARTTIME

-- Create an index that goes from 1, 2, ..., N
-- The index represents how early in the patient's stay a creatinine value was measured
-- Consequently, when we later select index == 1, we only select the first (admission) creatinine
-- In addition, we only select the first stay for the given SUBJECT_ID
, ROW_NUMBER ()
        OVER (PARTITION BY ie.ICUSTAY_ID
              ORDER BY CHARTTIME DESC
                  ) as rn
from first_icu ie
left join labevents le
  on ie.SUBJECT_ID = le.SUBJECT_ID
  and le.ITEMID = 50912
  and le.VALUENUM is not null
  and le.CHARTTIME between (ie.DISCHTIME - interval '96' hour) and (ie.DISCHTIME + interval '24' hour)
),
last_dis_creat as(
select * 
from all_dis_creats
where rn  = 1
)
select first_icu.*, first_serv.first_service, KDIGO_CREAT.admcreat, KDIGO_CREAT.admcreattime, last_dis_creat.discreat, KDIGO_CREAT.highcreat48hr, KDIGO_CREAT.highcreattime48hr, KDIGO_CREAT.highcreat7day, KDIGO_CREAT.highcreattime7day, rrt.rrt, esrd_icd.esrd, oasis.oasis, patients.dod
, Case WHEN first_icu.hospital_expire_flag = 1 THEN  0
  ELSE (date_part('Day', patients.dod-first_icu.dischtime))
  END
as day_after_icu_discharge_death
from first_icu
inner join
mimiciii.KDIGO_CREAT 
on KDIGO_CREAT.icustay_id = first_icu.icustay_id
join
mimiciii.rrt
on KDIGO_CREAT.icustay_id = rrt.icustay_id
join
mimiciii.oasis
on oasis.icustay_id = first_icu.icustay_id
left join mimiciii.patients
on patients.subject_id = first_icu.subject_id
join esrd_icd
on first_icu.hadm_id = esrd_icd.hadm_id
join last_dis_creat
on last_dis_creat.subject_id = first_icu.subject_id
join first_serv
on first_serv.hadm_id = first_icu.hadm_id
where first_icu.age >=18
order by first_icu.subject_id


    """)
except:
    print("I can't SELECT from noteevents")
    raise
print("Fetching notes.")
rows = cur.fetchall()



#aki_stage saves the stage of the AKI
aki_stage = AutoVivification()
#num_each_stage is the number of patients with each stage of AKI, with 0 = there is no KDIGO AKI and 4 there is no creatinine
num_each_stage = []
for i in range(5): #this sets each element of num_each_stage to 0
       num_each_stage.append(0)
num_each_recovery = []
for i in range(6): #this sets each element of num_each_recovery
       num_each_recovery.append(0)


	   

date_append = re.sub(r"[:|\s|\.]","-",str(datetime.datetime.now()))
write = output_path +'AKI_'+date_append+'.txt'
write = open(write, 'w')


# printing out titles for columns- It will need to be modified depending what columns you are printing
write.write(
				'subject_id'+"\t" 
				'hadm_id'+"\t" 
				'icustay_id' +"\t"
				'gender' +"\t"
				'los_hospital' +"\t"
				'age' +"\t"
				'ethnicity' +"\t"
				'admission_type'  +"\t"
				'first_service' +"\t"
				'hospital_expire_flag' +"\t"
				'los_icu' +"\t"
				'admcreat' +"\t"
				'discreat' +"\t"
				'highcreat48hr' +"\t"
				'highcreat7day' +"\t"
				'rrt' +"\t"
				'oasis' +"\t"
				'esrd' +"\t"
				'day_after_icu_discharge_death' +"\t"
				'AKI'  +"\t"
				"AKI_discharge\n"
)
# This goes through all the lines of the query
# We will first print out all the columns that we want and then we will calculate 1) level of AKI (if any) 2) death 1-30 days, 31-90, 91-365, >365
for row in rows:
	
	format_str = str(row['subject_id'])+"\t" + \
	str(row['hadm_id'])+"\t"+ \
	str(row['icustay_id'])+"\t"+ \
	row['gender']+"\t"+ \
	str(row['los_hospital']) +"\t" +\
	str(row['age']) +"\t" + \
	row['ethnicity'] +"\t" +\
	row['admission_type'] +"\t" +\
	row['first_service'] +"\t" +\
	str(row['hospital_expire_flag']) +"\t" +\
	str(row['los_icu']) +"\t" +\
	str(row['admcreat']) +"\t" +\
	str(row['discreat']) +"\t" +\
	str(row['highcreat48hr']) +"\t" +\
	str(row['highcreat7day']) +"\t" +\
	str(row['rrt']) +"\t" +\
	str(row['oasis']) +"\t" +\
	str(row['esrd']) +"\t" + \
	str(row['day_after_icu_discharge_death']) +"\t"
	write.write(format_str)
	if (row['admcreat'] is None):
		aki_stage[row['icustay_id']] = -1
		num_each_stage[4] = num_each_stage[4] + 1
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
			aki_stage[row['icustay_id']] = 0
			num_each_stage[0] = num_each_stage[0] + 1
	write.write(str(aki_stage[row['icustay_id']]) + "\t")
	
	#-1 = there was admit creatinine missing, or there was AKI and discharge creatinine is missing.
	#-1 = discharge creatinine is less than admit creatinine, but there was no AKI per KDIGO definition
	#0 = there was no AKI
	#1 = there was AKI then recovery
	#2 = there was AKI and no recovery
	#There is a thrid group that was never diagnosed with AKI, but leaves the hospital with a creatinine greater than the admit creatinine..
	#Not sure what we should do about those. Nothing currently, they are under 0 (no AKI)
	#If discharge creatinine was not documented and there was no AKI (per KDIGO), it will be 0
	if (row['admcreat'] is None):
	#if there is no admission creatinine
		write.write("-1")
		num_each_recovery[4] = num_each_recovery[4] + 1
	elif(row['discreat'] is None and (aki_stage[row['icustay_id']] > 0)):
	#if there is n discharge creatinine, but there was AKI, then -1
		write.write("-1")
	elif(row['discreat'] is None and (aki_stage[row['icustay_id']] == 0)):
		write.write("0")
	elif(row['discreat'] is not None):	
		if(((row['discreat']>(1.25 * row['admcreat']) or (row['discreat']>=4))  and (aki_stage[row['icustay_id']] > 0)) or row['rrt']==1):
			#if there was originally AKI and NO return to baseline (or cr >= 4)
			write.write("2")
			num_each_recovery[2] = num_each_recovery[2] +1
		elif(row['discreat']<=(1.25 * row['admcreat']) and (row['discreat']<4) and (aki_stage[row['icustay_id']] > 0)): 
			# if there was originally AKI and there was return to baseline (and cr  is less than 4)
			write.write("1")
			num_each_recovery[1] = num_each_recovery[1] + 1
		elif((row['admcreat'])>(row['discreat']*1.5) and (aki_stage[row['icustay_id']] == 0)):
			#If admission creatinine is greater than discharge creatinine * 1.5, but there was no AKI then 0- this is just here in case we want to edit in the future
			write.write("0")
			num_each_recovery[0] = num_each_recovery[0] +1
		elif(aki_stage[row['icustay_id']] == 0):
		#If there is no AKI at admission, then 0
			write.write("0")

#	elif(((row['discreat'])>(row['admcreat']*1.25)) and (aki_stage[row['icustay_id']] == 0)):
#		num_each_recovery[3] = num_each_recovery[3] + 1
#	else:
#		num_each_recovery[5] = num_each_recovery[5] + 1
	write.write("\n")

print(num_each_recovery[0])
print(num_each_recovery[1])
print(num_each_recovery[2])
print(num_each_recovery[3])
print(num_each_recovery[4])
print(num_each_recovery[5])
#

#make another column for 30 day mortality, 31-90, 91-365 days after discharge
#get rid of people who were esrd!! find icd9 code
# minimum creatinine lower than 25% of the admission creatinine
#dishtime



