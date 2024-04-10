
# dependencies
from openpyxl import load_workbook
from openpyxl.utils import get_column_interval
from openpyxl.utils.cell import coordinate_from_string as cfs
import pandas as pd
from datetime import datetime, timedelta, date



print('Starting job...\n')



# generate data frame
def convert_coordinates_to_dataframe(table_coordinates, sheet):
    col_start = cfs(table_coordinates.split(':')[0])[0]
    col_end = cfs(table_coordinates.split(':')[1])[0]

    # store table contents in array
    data_rows = []
    for row in sheet[table_coordinates]:
        data_rows.append([str(cell.value).replace(",", " ") for cell in row])
    
    # create dataframe from array
    df = pd.DataFrame(data_rows, columns = get_column_interval(col_start, col_end))

    df.columns = df.iloc[0] # Change header to first row
    df = df[1:]  # remove first row from DataFrame to remove the duplicate

    # df.insert(0,'report_exec_date')

    return df



wb          = load_workbook("C:\\Users\\l.haynes\\Desktop\\py_edit\\test.xlsx", data_only = True) # workbook (file)
df_dict     = {} # Dictionary to hold the dataframe for each table
df_dict2     = {}
ws          = wb['CSM Template'] # worksheet (use interval for worksheet name)
ws2          = wb['Settings'] # worksheet (use interval for worksheet name)



### Get the table coordinates from the worksheet table dictionary
for tblname, tblcoord in ws.tables.items():
    print(f"Table Name: {tblname}, Coordinate: {tblcoord}\n")
    df_dict[tblname] = convert_coordinates_to_dataframe(tblcoord, ws)  # Convert to dataframe


for tblname, tblcoord in ws2.tables.items():
    print(f"Table Name: {tblname}, Coordinate: {tblcoord}\n")
    df_dict2[tblname] = convert_coordinates_to_dataframe(tblcoord, ws2)  # Convert to dataframe


# add spacer in output
print("\n")




### Print the DataFrames
for table_name, df in df_dict.items():
    
    if table_name == 'log':
        print(f"DataFrame from Table '{table_name}'\n\n")
        bar = df
        bar['id'] = 1
        # print(df.to_dict('records'))
        # print(df.columns.values)

for table_name, df2 in df_dict2.items():
    
    if table_name == 'staff':
        print(f"DataFrame from Table '{table_name}'\n\n")
        
        df2.set_index("Name", inplace = True, drop = False) 
        foo = df2.loc[["Vaughn Haynes"]]
        
        # foo = df2.iloc[[0]]

        # foo = df2
        foo['id'] = 1
        # print(foo.to_dict())
        # print(df2.to_dict('records'))
        # print(df2.columns.values)

print(foo)


m = foo.merge(bar, on='id')
m = m.drop('VTS', axis=1)
m = m.drop('id', axis=1)
m = m.drop('Total', axis=1)
m = m.replace(to_replace="None",value="0")

# Using drop() function to delete last row
m.drop(index=m.index[-1],axis=0,inplace=True)

print(m)

m.to_csv("C:\\Users\\l.haynes\\Desktop\\py_edit\\output.csv", header = True, index = False)


print('Done!')

