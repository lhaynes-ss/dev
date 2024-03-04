'''
===================================
Upload Paramount+ and Pluto files
to AWS 
$ aws --profile nyc s3 ls s3://samsung.ads.data.share/analytics/custom/vaughn/test/python/weekly/
===================================
'''
# dependencies
import json
import boto3
import _config as cfg


print('Starting job...\n')

# get configs
app_config  = cfg.get_app_config()
path        = app_config['path']


# get json file
with open(f"{path}output\\upload_list.json") as input_file:
    file_dict = json.load(input_file)
    
    # print(file_dict)


    # get s3 connection
    print("Connecting to s3...")
    session = boto3.Session(profile_name='nyc')
    s3 = session.client('s3')
    print("Connected!\n")


    # upload to s3
    for f_name, f_dictionary in file_dict.items():

        f_path      = f_dictionary['output_file_path']
        s3_bucket   = f_dictionary['s3_bucket']
        s3_prefix   = f_dictionary['s3_prefix']
        interval    = f_dictionary['interval']

        print(f_path)
        print(f"Copying {f_name}...")
        s3.upload_file(f_path, s3_bucket, f"{s3_prefix}{interval}/{f_name}")
        print(f"File Copied.\n")


print('Done!')


