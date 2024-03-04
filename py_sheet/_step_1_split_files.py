'''
===================================
Split Paramount+ and Pluto Spreadsheets
into separate files 
===================================
'''
# dependencies
from openpyxl import load_workbook
from openpyxl.utils import get_column_interval
from openpyxl.utils.cell import coordinate_from_string as cfs
import pandas as pd
import json
import _config as cfg


print('Starting job...\n')


# get configs
app_config      = cfg.get_app_config()
report_config   = cfg.get_report_config()


start_date  = report_config['start_date']
end_date    = report_config['end_date']


# io variables
path                        = app_config['path']
destination_bucket_keys     = app_config['destination_bucket_keys']
file_dict   = {} # dictionary to hold output files for s3


def convert_coordinates_to_dataframe(table_coordinates, sheet):
    col_start = cfs(table_coordinates.split(':')[0])[0]
    col_end = cfs(table_coordinates.split(':')[1])[0]

    # store table contents in array
    data_rows = []
    for row in sheet[table_coordinates]:
        data_rows.append([cell.value for cell in row])
    
    # create dataframe from array
    df = pd.DataFrame(data_rows, columns = get_column_interval(col_start, col_end))

    df.columns = df.iloc[0] # Change header to first row
    df = df[1:]  # remove first row from DataFrame to remove the duplicate

    return df


for report in report_config['reports']:
    
    # report variables
    partner     = report['partner'] # 'paramount_plus'
    region      = report['region'] # uk
    interval    = report['interval'] # weekly
    filename    = report['filename']
    bucket_key  = report['bucket_key']

    s3_bucket   = destination_bucket_keys[bucket_key]['bucket']
    s3_prefix   = destination_bucket_keys[bucket_key]['prefix']

    wb          = load_workbook(path + filename, data_only = True) # workbook (file)
    ws          = wb[interval] # worksheet (use interval for worksheet name)
    df_dict     = {} # Dictionary to hold the dataframe for each table


    ### Get the table coordinates from the worksheet table dictionary
    for tblname, tblcoord in ws.tables.items():
        print(f"Table Name: {tblname}, Coordinate: {tblcoord}")
        df_dict[tblname] = convert_coordinates_to_dataframe(tblcoord, ws)  # Convert to dataframe


    # add spacer in output
    print("\n")


    ### Print the DataFrames
    for table_name, df in df_dict.items():
        
        # print(f"DataFrame from Table '{table_name}'")
        # print(df)

        output_file = f"{partner}_{region}_{interval}_{table_name}_{start_date}_{end_date}.csv"
        output_file_path = f"{path}output\\{output_file}"

        file_dict[output_file] = {
            'output_file_path': output_file_path
            ,'s3_bucket': s3_bucket
            ,'s3_prefix': s3_prefix
            ,'interval': interval
        }

        print(f"Building {output_file_path}...")
        df.to_csv(output_file_path, header = True, index = False)
        print("Build complete.\n")


# save file dictionary as json
with open(f"{path}output\\upload_list.json", "w") as output_file: 
    json.dump(file_dict, output_file)


print('Done!')

