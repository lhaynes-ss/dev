'''
Org Time Tracking
---------------------
Objective is to distribute excel time tracking templates. End users will log time spent for tasks for 5 weeks. 
At the end of that period we will collect the data for reporting.

https://adgear.atlassian.net/browse/SAI-6348

This program extracts time data from xlsx spreadsheets for import in UDW.
'''

import glob
import pandas as pd 
from _extract import extract_file

print('Starting batch process...')


# set variables
app_path    = r'C:\Users\l.haynes\Desktop\py_edit'


# try to get file list
documents   = set()
dir_path    = fr'{app_path}\files\**\*.xlsx'
output_path = fr'{app_path}\output'

# to keep track of files not able to be extracted
fail_list = []


for file in glob.glob(dir_path, recursive = True):
    documents.add(file)

# check to see if we have any files
documents_length = len(documents)

if documents_length == 0:
    print("The files couldn't be read.")
    print('Done!')

    quit()


# loop through each file
for file in documents:
    
    # print(file)

    try:
        extract_file(file, output_path)
    except:
        fail_list.append(file)
        print(f"Skipping {file}...")


# save failures
if len(fail_list) > 0:
    df = pd.DataFrame(fail_list)
    df.to_csv(f"{output_path}/failures.csv", header = False, index = False)


print('Done!')

