# -*- coding: utf-8 -*-

# Member ID: 'DESYNPUF_ID'

import csv
import re
from datetime import date, datetime

year_reg = re.compile('2009.*')
db_reg = re.compile('250.*')

## The import + filter should be transformed into a function if pandas isn't an option

# CLM_FROM_DT
infile = open("C:\\Users\\rruther1\\Documents\\SynPuf_Medicare\\bhe_medicare_synthetic_data_test\\raw\\inpatient\\inpatient.txt", "r")
inpat_dict = csv.DictReader(infile,delimiter=',')

# CLM_FROM_DT
infile = open("C:\\Users\\rruther1\\Documents\\SynPuf_Medicare\\bhe_medicare_synthetic_data_test\\raw\\outpatient\\outpatient.txt", "r")
outpat_dict = csv.DictReader(infile,delimiter=',')

# CLM_FROM_DT
infile = open("C:\\Users\\rruther1\\Documents\\SynPuf_Medicare\\bhe_medicare_synthetic_data_test\\raw\\carrier\\carrier.txt", "r")
carrier_dict = csv.DictReader(infile,delimiter=',')

### Inpatient Munging
dx_pat = set() # set to get unique patients
dx_pat_dt = {}# dict to get patient IDs and dates
inpat = []
outpat = []
carrier = []

def add_years(d, years):
    """Return a date that's `years` years after the date (or datetime)
    object `d`. Return the same calendar date (month and day) in the
    destination year, if it exists, otherwise use the following day
    (thus changing February 29 to March 1).
    https://stackoverflow.com/questions/15741618/add-one-year-in-current-date-python/15742722
    """
    try:
        return d.replace(year = d.year + years)
    except ValueError:
        return d + (date(d.year + years, 1, 1) - date(d.year, 1, 1))

def add_pt_dt(dct,memid,dt):
    if memid in dct:
        dct[memid].append(datetime.strptime(dt,'%Y%m%d').date())
    else:
        dct[memid] = [datetime.strptime(dt,'%Y%m%d').date()]

### 2009 Year Filters for each dataset. 
for row in inpat_dict:
    if re.match(year_reg,row['CLM_FROM_DT']) is not None:
        inpat.append(row)
        
for row in outpat_dict:
    if re.match(year_reg,row['CLM_FROM_DT']) is not None:
        outpat.append(row)
        
for row in carrier_dict:
    if re.match(year_reg,row['CLM_FROM_DT']) is not None:
        carrier.append(row)

# Inpatient filter
for i in inpat:
    dx_list=[]
    filter = {key: i[key] for key in i.keys() & {'ADMTNG_ICD9_DGNS_CD','ICD9_DGNS_CD_1','ICD9_DGNS_CD_2','ICD9_DGNS_CD_3','ICD9_DGNS_CD_4','ICD9_DGNS_CD_5','ICD9_DGNS_CD_6','ICD9_DGNS_CD_7','ICD9_DGNS_CD_8','ICD9_DGNS_CD_9','ICD9_DGNS_CD_10'}} 
    for key, value in filter.items():
        dx_list.append(value)
    for dx in dx_list: 
        if re.match(db_reg,dx) is not None:
            dx_pat.add(i['DESYNPUF_ID'])
            add_pt_dt(dx_pat_dt,i['DESYNPUF_ID'],i['CLM_FROM_DT'])

# Outpatient filter (no Dx Key changes from outpatient)
for i in outpat:
    dx_list=[]
    filter = {key: i[key] for key in i.keys() & {'ADMTNG_ICD9_DGNS_CD','ICD9_DGNS_CD_1','ICD9_DGNS_CD_2','ICD9_DGNS_CD_3','ICD9_DGNS_CD_4','ICD9_DGNS_CD_5','ICD9_DGNS_CD_6','ICD9_DGNS_CD_7','ICD9_DGNS_CD_8','ICD9_DGNS_CD_9','ICD9_DGNS_CD_10'}} 
    for key, value in filter.items():
        dx_list.append(value)
    for dx in dx_list: 
        if re.match(db_reg,dx) is not None:
            dx_pat.add(i['DESYNPUF_ID'])
            add_pt_dt(dx_pat_dt,i['DESYNPUF_ID'],i['CLM_FROM_DT'])
            

# Carrier filter (Dx key change. 1-9 + Line_Dx)
for i in carrier:
    dx_list=[]
    filter = {key: i[key] for key in i.keys() & {'ICD9_DGNS_CD_1','ICD9_DGNS_CD_2','ICD9_DGNS_CD_3','ICD9_DGNS_CD_4','ICD9_DGNS_CD_5','ICD9_DGNS_CD_6','ICD9_DGNS_CD_7',
              'LINE_ICD9_DGNS_CD_1',
              'LINE_ICD9_DGNS_CD_2',
              'LINE_ICD9_DGNS_CD_3',
              'LINE_ICD9_DGNS_CD_4',
              'LINE_ICD9_DGNS_CD_5',
              'LINE_ICD9_DGNS_CD_6',
              'LINE_ICD9_DGNS_CD_7',
              'LINE_ICD9_DGNS_CD_8',
              'LINE_ICD9_DGNS_CD_9',
              'LINE_ICD9_DGNS_CD_10',
              'LINE_ICD9_DGNS_CD_11',
              'LINE_ICD9_DGNS_CD_12',
              'LINE_ICD9_DGNS_CD_13'
}} 
    for key, value in filter.items():
        dx_list.append(value)
    for dx in dx_list: 
        if re.match(db_reg,dx) is not None:
            dx_pat.add(i['DESYNPUF_ID'])
            add_pt_dt(dx_pat_dt,i['DESYNPUF_ID'],i['CLM_FROM_DT'])
            
            
## Total Diabetes Patients
print(len(dx_pat))

dx_index_dict={}
## Get min and index
for key in dx_pat_dt:
    indx_dt = min(dx_pat_dt[key])
    end_dt = add_years(indx_dt,1)
    dx_index_dict[key]=(indx_dt,end_dt)

##
rx_pats = []  
rx_set = set()  
## Read in meds and filter for Lovastatin
infile = open("C:\\Users\\rruther1\\Documents\\SynPuf_Medicare\\bhe_medicare_synthetic_data_test\\raw\\prescription\\prescription.txt", "r")
rx_dict = csv.DictReader(infile,delimiter=',')    

## read in Lovastatin NDCs
ndclist = [line.rstrip('\n') for line in open("C:\\Users\\rruther1\\Documents\\SynPuf_Medicare\\bhe_medicare_synthetic_data_test\\lookup\\lovastatin.txt")]

## Members in Diabetes Cohort, Members have NDC, Date is within span. 
for row in rx_dict:
    if (row['DESYNPUF_ID'] in dx_pat and row['PROD_SRVC_ID'] in ndclist):
        srvcdt = datetime.strptime(row['SRVC_DT'],'%Y%m%d').date()
        indx_dt = dx_index_dict[row['DESYNPUF_ID']][0]
        end_dt = dx_index_dict[row['DESYNPUF_ID']][1]
        if (srvcdt>=indx_dt and srvcdt <=end_dt):
            rx_pats.append(row)
            rx_set.add(row['DESYNPUF_ID'])

# Total Diab Pats with targeted statin within 1 year of index.
print(len(rx_set))

## Read in member demographics. 
age_set = set()
debug_lst = []
infile = open("C:\\Users\\rruther1\\Documents\\SynPuf_Medicare\\bhe_medicare_synthetic_data_test\\raw\\beneficiary/beneficiary.txt", "r")
member_dict = csv.DictReader(infile,delimiter=',')    

## 'BENE_BIRTH_DT'
## Members in Diabetes Cohort, Members have NDC, Date is within span. 
for row in member_dict:
    if (row['DESYNPUF_ID'] in rx_set):
        dob = datetime.strptime(row['BENE_BIRTH_DT'],'%Y%m%d').date()
        age_cut = add_years(dob,65)
        debug_lst.append(row)
        if dx_index_dict[row['DESYNPUF_ID']][0]>=age_cut:
            age_set.add(row['DESYNPUF_ID'])

print(len(age_set))