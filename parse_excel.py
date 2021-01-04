#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse Covid-19 hospital activity xlsx file

Admissions and beds data is weekly file from: 
https://www.england.nhs.uk/statistics/statistical-work-areas/covid-19-hospital-activity/

Jan 02 2021

@author: jah-photoshop
"""

import os,csv
from datetime import datetime, timedelta
from os import listdir
from os.path import isfile,join
import d6tstack.convert_xls

def merge_columns(source,destination):
    for ind,el in enumerate(destination[7:]):
        if(el=='-'):
            if source[ind+7]!='-':
                destination[ind+7]=source[ind+7]
            
    
#Parse a CSV file and read data lines into list
def read_file(filename, delim=','):
    data = []
    if(debug): print ("Opening file %s" % (filename))
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delim)
        for row in csv_reader:
          data.append(row)
    if(debug): print(f'Processed {len(data)} lines.')
    return (data)

def write_csv(filename,data):
    with open(filename,'w') as f:
        for line in data:
            for ind,el in enumerate(line):
                if(ind>0): f.write(";")
                f.write("%s" % el)
            f.write("\n")

print("________________________________________________________________________________")
print("Covid NHS Data       Plotter    -    version 1.0    -    @jah-photoshop Jan 2021")
print("________________________________________________________________________________")

debug=True
beds_data_filename = "Weekly-covid-admissions-and-beds-publication-201231.xlsx"
deaths_data_filename= "COVID-19-total-announced-deaths-3-January-2021.xlsx"
long_data_filename = "Covid-Publication-10-12-2020.xlsx"
postcode_lookup="etr.csv"
postcode_gr_lookup="postcode_lookup.csv"
print("Loading NHS postcode lookup %s" % postcode_lookup)
postcode_data = read_file(postcode_lookup)
postcodes = {line[0]:line[9] for line in postcode_data}
print("Loading NHS postcode GR lookup %s" % postcode_gr_lookup)
postcode_gr_data = read_file(postcode_gr_lookup)
postcodes_x = {line[0]:line[1] for line in postcode_gr_data}
postcodes_y = {line[0]:line[2] for line in postcode_gr_data}
csv_path = "csv"
csv_deaths_path = "csv"+os.path.sep+"deaths"
csv_long_path = "csv"+os.path.sep+"monthly"
stripped_csv_path = "csv_stripped"
stripped_long_path = stripped_csv_path + os.path.sep + "long"
if(not os.path.isdir(csv_deaths_path)): os.makedirs(csv_deaths_path)
if(not os.path.isdir(stripped_csv_path)): os.makedirs(stripped_csv_path)
if(not os.path.isdir(stripped_long_path)): os.makedirs(stripped_long_path)

if(not os.path.isdir(csv_long_path)): os.makedirs(csv_long_path)


print("Converting "+ beds_data_filename + " into .CSV files")
#xls = pd.ExcelFile(beds_data_filename)
#sheetnames = xls.sheet_names
#sheets=[]
#for sn in sheetnames:
#    sheet = pd.read_excel(xls,sn)
#    sheets.append(sheet)

c = d6tstack.convert_xls.XLStoCSVMultiSheet(beds_data_filename,output_dir=csv_path)
c.convert_all()

csv_files = [f for f in listdir(csv_path) if isfile(join(csv_path,f))]
csv_files.sort()
earliest_start_date = datetime(2020,12,31)
latest_end_date = datetime(2020,1,1)
trust_codes = []
trust_names = []
trust_regions = []
categories=[]
for ind,file in enumerate(csv_files):
    data=read_file(csv_path+os.path.sep+file)
    stripped_data = ['']

    for line in data:
        if line[0]=='Yes':# or line[0]=='No': 
            if line[2] not in trust_codes:
                trust_codes.append(line[2])
                trust_regions.append(line[1])
                trust_names.append(line[3])
            stripped_data.append(line[1:])
        if line[0]=='Type 1 Acute?':
            start_date = datetime.strptime(line[4][0:10],'%Y-%m-%d')
            end_date = datetime.strptime(line[-1][0:10],'%Y-%m-%d')
            if start_date < earliest_start_date: earliest_start_date = start_date
            if end_date > latest_end_date: latest_end_date = end_date
    stripped_filename = file.split('-')[-1]
    header=[stripped_filename[:-4],'Code','Trust']
    categories.append(stripped_filename[:-4])
    for i in range((end_date - start_date).days + 1):
        header.append(datetime.strftime(start_date + timedelta(days=i),'%Y-%m-%d'))
    stripped_data[0]=header
    a=stripped_filename.strip()
    stripped_cfn = stripped_csv_path+os.path.sep+stripped_filename.replace(' ','')
    write_csv(stripped_cfn,stripped_data)
#    with open(stripped_cfn,'w') as f:
#        for line in stripped_data:
#            for ind,el in enumerate(line):
#                if(ind>0): f.write(";")
#                f.write("%s" % el)
#            f.write("\n")

no_trusts = len(trust_codes)
print("Number of CC Trusts:%d  Start date:%s  End date:%s" % (no_trusts,earliest_start_date.strftime('%Y-%m-%d'),latest_end_date.strftime('%Y-%m-%d')))


#Add the deaths file
print("Converting " + deaths_data_filename + " into .CSV files")
cd = d6tstack.convert_xls.XLStoCSVMultiSheet(deaths_data_filename,output_dir=csv_deaths_path)
cd.convert_all()
deaths_csv_filename = csv_deaths_path + os.path.sep + deaths_data_filename+"-Tab4 Deaths by trust.csv"
deaths_csv = read_file(deaths_csv_filename)
deaths_header=deaths_csv[13]
deaths_data=deaths_csv[14:]
deaths_days = len(deaths_header)-7
run_in_column = deaths_header[4]
earliest_start_date=datetime(2020,3,1)
latest_end_date = earliest_start_date + timedelta(days=deaths_days)


no_days = (latest_end_date - earliest_start_date).days + 1


#Add the monthly file
print("Converting " + long_data_filename + " into .CSV files")
cd = d6tstack.convert_xls.XLStoCSVMultiSheet(long_data_filename,output_dir=csv_long_path)
cd.convert_all()
m_csv_files = [f for f in listdir(csv_long_path) if isfile(join(csv_long_path,f))]
m_csv_files.sort()
m_categories = []
for file in m_csv_files:
    stripped_filename=file[len(long_data_filename)+1:]
    m_cat = stripped_filename[:-4]
    if(m_cat != 'Summary'):
        m_categories.append(m_cat)
        data = read_file(csv_long_path+os.path.sep+file)
        stripped_data=['']
        d_header=[m_cat,'Code','Trust']
        in_header = data[10]
        s_date = datetime.strptime(in_header[4][0:10],'%Y-%m-%d')
        e_date = s_date+timedelta(days=len(in_header) + 5)
        d_no_days = (e_date - s_date).days + 1
        for n in range(d_no_days):
            d_header.append(datetime.strftime(s_date + timedelta(days=n),'%Y-%m-%d'))
        stripped_data[0]=d_header
        for line in data:
            if line[2] in trust_codes:
                tci = trust_codes.index(line[2])
                n_line = [trust_regions[tci],line[2],trust_names[tci]]
                n_line.extend(line[4:])
                stripped_data.append(n_line)
        stripped_mfn = stripped_long_path + os.path.sep + stripped_filename.replace(' ','')
        write_csv(stripped_mfn,stripped_data)
short_cats = len(categories)
categories.extend(m_categories)                




categories.append('Deaths')
categories.append('Cumulative Deaths')
cats = len(categories)


columns = no_days + 8

o_columns = ['Admissions','Diagnoses','Admissions+Diagnoses','Covid Beds','MV Beds','Deaths','Discharges','Cum Adm+Diag','Cum Deaths','Cum Discharges']
lines = cats * no_trusts
header = ['Code','Trust Name','Region','Post-Code','X','Y','Measure',run_in_column]
for i in range(no_days):
    header.append(datetime.strftime(earliest_start_date + timedelta(days=i),'%Y-%m-%d'))
#Create blank array
combined_data = []
for i in range(lines):
    ind = int(i / cats)
    off = i % cats
    pc = postcodes.get(trust_codes[ind])
    pc_x = postcodes_x.get(pc)
    pc_y = postcodes_y.get(pc)
    sub_line = [trust_codes[ind],trust_names[ind],trust_regions[ind],pc,pc_x,pc_y,categories[off],'-']
    for q in range(no_days):
        sub_line.append('-')
    combined_data.append(sub_line)
    
print("Filling data")
trimmed_files = [f for f in listdir(stripped_csv_path) if isfile(join(stripped_csv_path,f))]
trimmed_files.sort()
for ind,file in enumerate(trimmed_files):
    data=read_file(stripped_csv_path+os.path.sep+file,delim=';')
    lheader=data[0]
    s_date = datetime.strptime(lheader[3],"%Y-%m-%d")
    s_offset = (s_date - earliest_start_date).days
    for line in data[1:]:
        index = trust_codes.index(line[1])
        row = (index * cats) + ind
        for col, ent in enumerate(line[3:]):
            o_col = col + s_offset + 8
            combined_data[row][o_col]=ent

trimmed_long_files = [f for f in listdir(stripped_long_path) if isfile(join(stripped_long_path,f))]
trimmed_long_files.sort()
for ind,file in enumerate(trimmed_long_files):
    data=read_file(stripped_long_path+os.path.sep+file,delim=';')
    lheader=data[0]
    s_date = datetime.strptime(lheader[3],"%Y-%m-%d")
    s_offset = (s_date - earliest_start_date).days
    for line in data[1:]:
        index = trust_codes.index(line[1])
        row = (index * cats) + ind + short_cats
        for col, ent in enumerate(line[3:]):
            o_col = col + s_offset + 8
            combined_data[row][o_col]=ent

                
#Fill in deaths data
for line in deaths_data:
    if line[2] in trust_codes:
        index=trust_codes.index(line[2])
        row = (index * cats) + cats - 2
        cum_count = 0
        for col, ent in enumerate(line[4:-2]):
            o_col = col + 7
            combined_data[row][o_col]=ent
            cum_count += int(ent)
            combined_data[row+1][o_col]=cum_count

o_data = [header]
o_data.extend(combined_data)
write_csv('combined.csv',o_data)


o_columns = ['Admissions','Diagnoses','New Cases','Covid Beds','MV Beds','Deaths','Discharges','Cum Cases','Cum Deaths','Cum Discharges']
xref=[[15],[23],[35,9],[3,37],[8,34],[39],[29],[],[40]]
q_cats =len(o_columns)
s_data = []
qlines = q_cats * no_trusts
for i in range(qlines):
    ind = int(i / q_cats)
    off = i % q_cats
    pc = postcodes.get(trust_codes[ind])
    pc_x = postcodes_x.get(pc)
    pc_y = postcodes_y.get(pc)
    sub_line = [trust_codes[ind],trust_names[ind],trust_regions[ind],pc,pc_x,pc_y,o_columns[off],'-']
    for q in range(no_days):
        sub_line.append('-')
    s_data.append(sub_line)
    
merge_list = []
for i in range(no_trusts):
    ix=i*cats
    lx=i*q_cats
    trust_data=combined_data[ix:ix+cats]
    for pos,xr in enumerate(xref):
        for el in xr:
            merge_columns(combined_data[ix + el],s_data[lx + pos])
    #Add to cases data [where blank] with combination of admissions + diagnosis shifted << 1 day
    for ind,el in enumerate(s_data[lx+2][7:]):
        if(el=='-'):
            nc = 0
            if(s_data[lx][7+ind])!='-': nc+=int(s_data[lx][7+ind])
            if(ind+8 < len(s_data[lx+1])): 
                if s_data[lx+1][8+ind]!='-': nc+=int(s_data[lx+1][8+ind])
            if nc > 0: s_data[lx+2][7+ind]=nc
    #Add cumulative cases and discharges
    c_count=0
    for ind,el in enumerate(s_data[lx+2][7:]):
        if(el!='-'):
              c_count+=int(el)
        if(c_count>0): s_data[lx+7][7+ind]=c_count
    d_count=0
    for ind,el in enumerate(s_data[lx+6][7:]):
        if(el!='-'):
              d_count+=int(el)
        if(d_count>0): s_data[lx+9][7+ind]=d_count        

s_data_h = [header]
s_data_h.extend(s_data)
write_csv('compacted.csv',s_data_h)