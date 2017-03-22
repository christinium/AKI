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


#set values
def set_hash(array):
#set values 
	prev_id = {}
	prev_id['hadm_id'] = array[1]
	prev_id['subject_id'] = array[0]
	prev_id['gender'] = array[6]
	prev_id['dob'] = array[7]
	prev_id['dod'] = array[8]
	prev_id['age'] = array[9]
	prev_id['day_death'] = array[10]
	prev_id['hospital_expire_flag'] = array[11]
	prev_id['admittime'] = array[12]
	prev_id['dischtime'] = array[13]
	prev_id['discharge_location'] = array[14]
	prev_id['admission_type'] = array[15]
	prev_id['ethnicity'] = array[16]
	return prev_id

#print and calc AKI
def print_all(prev_id, current, hosp):	
	aki = 0
	if ((current['max2d']> (baseline + 0.3)) or current['max'] > (baseline * 2)):
		aki = 1
		print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (prev_id['subject_id'], prev_id['hadm_id'], 
		hosp, current['baseline'], current['max'], current['max2d'], aki, current['max_charttime'], current['max2d_charttime'],
		prev_id['gender'], prev_id['dob'], prev_id['dod'], prev_id['age'],	prev_id['day_death'], 
		prev_id['hospital_expire_flag'], prev_id['admittime'], prev_id['dischtime'], prev_id['discharge_location'],	
		prev_id['admission_type'],	prev_id['ethnicity'])
#		print "wtf"
#	else:
#	print "%s\t%s\t%s\t%s" % (prev_id['hadm_id'], prev_id['subject_id'], baseline, current['max'])




path = 'C:\Python27\AKI\\'
file =  path+'head_all_creatinines_7_days_2015_11_24b_minus.txt'
#f = open(file, 'rt')

with open(file, 'r') as content_file:
    lines = content_file.read().splitlines()

#initialize
hadm_id = -1
subject_id = -1
baseline = -1
current = {};
current['baseline'] = -1
current['max_charttime'] = ''
current['max2d_charttime'] = ''
current['max2d'] = -1
current['max'] = -1
hosp = 1 #this is the number hospitalization for a particular subject_id

prev_id = {}
prev_id['hadm_id'] = -1
prev_id['subject_id'] = -1
prev_id['gender'] = ''
prev_id['dob'] = ''
prev_id['dod']= ''
prev_id['age'] = ''
prev_id['day_death'] = ''
prev_id['hospital_expire_flag'] = ''
prev_id['admittime'] = ''
prev_id['dischtime'] = ''
prev_id['discharge_location'] =''
prev_id['admission_type'] =''
prev_id['ethnicity'] = ''


#for line in f:
for count in range(0, len(lines)):

	# splitting comma deliminated file`
	x = lines[count].split(',')	
	if (int(x[9])>17 and x[3] != ''):
		# set the current subject_id and hadm_id
		subject_id = x[0]
		hadm_id = x[1]
		
	
		## if this is a new hadm_id then:
		if (hadm_id != prev_id['hadm_id']):		
#
			#print out previous max if it isn't the first item
			if (count != 0):
				print_all(prev_id, current, hosp)

			# set everything to baseline
			hosp = 1

			current['baseline'] = float(x[3])
			current['date_48h'] =  datetime.datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=2)
			current['date_7d']  = datetime.datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=7)
			current['max'] = current['baseline']
			current['max2d'] = current['baseline']
			current['max_charttime'] = x[2]
			current['max2d_charttime'] = x[2]
			## can save all the other things too like date and time etc
			#prev_id['hadm_id'] = hadm_id
			#### here print out max if it is not the very first one
			if (subject_id == prev_id['subject_id']):
				hosp = hosp + 1
#				print "hihihi%s\t%s\t%s\t%s\t%s" % (subject_id, prev_id['subject_id'], hadm_id, prev_id['hadm_id'], hosp)
				# it is a brand new patient, first hospitalization
			prev_id = set_hash(x)

		else:		
			#else it is not the first entry, find the max cr
			if ((float(x[3]) > current['max']) and (datetime.datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S")<current['date_7d'])):
				current['max'] = float(x[3])
				current['max_charttime'] = x[2]
			if ((float(x[3]) > current['max2d']) and (datetime.datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S")<current['date_48h'])):
				current['max2d'] = float(x[3])
				current['max2d_charttime'] = x[2]
		# reset all the things like date/time etc
		
####printing the last item ADD BACK!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#print_all(prev_id, current, hosp, baseline)
#print "%s\t%s\t%s\t%s\t%s\t%s" % (prev_id['subject_id'], prev_id['hadm_id'], hosp, baseline, current['max_charttime'], current['max2d_charttime'])
##NOTES
#
# - DOnt' forget to uncomment the last print
# - select 8 days from the sql query
#
#
#