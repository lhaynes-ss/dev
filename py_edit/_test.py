import os.path 

path = '/home/User/Documents/file.txt'
dirname = os.path.basename(os.path.dirname(path)) 

print(dirname)

