import glob
import pandas as pd 
from _extract import extract_file

# try to get file list
documents   = set()
dir_path    = r'C:\Users\l.haynes\Desktop\py_edit\files\**\*.xlsx'
output_path = r'C:\Users\l.haynes\Desktop\py_edit\output'

fail_list = []


for file in glob.glob(dir_path, recursive = True):
    documents.add(file)

# check to see if we have any files
documents_length = len(documents)

if documents_length == 0:
    print("The files couldn't be read.")
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

