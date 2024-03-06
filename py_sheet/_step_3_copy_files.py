'''
===================================
Copy Paramount+ and Pluto files
via UDW 
===================================
'''
# dependencies
import json
import snowflake.connector
import numpy as np
import _config as cfg


print('Starting job...\n')

# get configs
app_config  = cfg.get_app_config()
path        = app_config['path']


# ctx = context
print('Opening connection...')
ctx = snowflake.connector.connect(
    user            = 'l.haynes@partner.samsung.com'
    ,account        = 'adgear-udw_us_prd'
    ,authenticator  = 'externalbrowser'
)

# cs = cursor
cs = ctx.cursor()


# get json file
with open(f"{path}output\\udw_list.json") as input_file:
    udw_dict = json.load(input_file)



    for f_name, f_dictionary in udw_dict.items():
        f_to        = f_dictionary['to']
        f_from      = f_dictionary['from']
        f_cols      = f_dictionary['cols']
        col_string  = ''


        # print(f_from)
        # print(f_to)
        # print(f_cols)


        for c in np.array(f_cols):
            col_string = col_string + f",{c} VARCHAR(512)\n"


        # query to execute
        multi_statement_sql = f'''
        EXECUTE IMMEDIATE $$  
        BEGIN

            USE ROLE udw_clientsolutions_default_consumer_role_prod;
            USE WAREHOUSE udw_clientsolutions_default_wh_prod;
            USE DATABASE udw_prod;

            DROP TABLE IF EXISTS input_data_s3;
            CREATE TEMP TABLE input_data_s3 (
                {col_string[1:]}
            );
            COPY INTO input_data_s3
            FROM '{f_from}'
            STORAGE_INTEGRATION = DATA_ANALYTICS_SHARE
            FILE_FORMAT = (format_name = adbiz_data.analytics_csv);

            -- SELECT * FROM input_data_s3 LIMIT 100;

            -- copy to new s3 location
            COPY INTO {f_to} FROM input_data_s3
            file_format = (format_name = adbiz_data.mycsvformat999 compression = 'none')
                single = TRUE
                header = TRUE
                max_file_size = 4900000000
                OVERWRITE = TRUE
            ;
            
        END;
        $$
        '''

        print(f"Copying {f_name}...")
        # execute sql
        cs.execute(multi_statement_sql)
        print("Copied!\n")
        # print(multi_statement_sql)


print('Closing connection...')
cs.close()
print('Done!')


