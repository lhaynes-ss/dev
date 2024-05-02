'''
===================================
Print AWS
===================================
'''
# dependencies
import json
import _config as cfg


print('Starting job...\n')

# get configs
app_config  = cfg.get_app_config()
path        = app_config['path']

udw_dict    = {}
content     = ""


# get json file
with open(f"{path}output\\upload_list.json") as input_file:
    file_dict = json.load(input_file)
    
    # print(file_dict)



    # get files
    for f_name, f_dictionary in file_dict.items():

        f_path      = f_dictionary['output_file_path']
        s3_bucket   = f_dictionary['s3_bucket']
        s3_prefix   = f_dictionary['s3_prefix']
        interval    = f_dictionary['interval']
        stage       = f_dictionary['stage']
        cols        = f_dictionary['cols']

        # build a list of "copy to" and "copy from" paths for UDW
        if stage != '':
            udw_dict[f_name] = {
                'to': f"{stage}{interval}/{f_name}" 
                ,'from': f"s3://{s3_bucket}/{s3_prefix}{interval}/{f_name}"
                ,'cols': cols
             }


        content = f"{content}aws --profile scop s3 ls s3://{s3_bucket}/{s3_prefix}{interval}/{f_name}\n"


f = open(f"{path}output\\aws.txt", "w")
f.write(content)
f.close()
print('Done!')


