#!/usr/bin/python2.4
# 11/23/2015
# subject_id, hadm_id, gender, dob, dod, age, hospital_expire_flag, admittime,  
# dischtime, deathtime, discharge_location, admission_type, ethnicity, charttime, 
# value, valuenum, uom
#
# the file is sorted on subject_id, hadm_id, and charttime
# first line is label
import re 
import datetime


#date_str = "2008-11-10 17:53:59"
date_str = "2156-01-25 01:00:00"
time = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
end_time = time + datetime.timedelta(days=2)
print "%s\t%s\t%s\t%s"% (repr(end_time), repr(time), end_time, time)



