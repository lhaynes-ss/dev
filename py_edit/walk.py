import glob

# try to get file list
documents = set()
dir_path = r'C:\Users\l.haynes\Desktop\py_edit\files\**\*.xlsx'

for file in glob.glob(dir_path, recursive=True):
    documents.add(file)

# check to see if we have any files
documents_length = len(documents)

if documents_length == 0:
    print("The files couldn't be read.")
    quit()


# loop through each file
for file in documents:
    print(file)

