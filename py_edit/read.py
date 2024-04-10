
# dependencies
from openpyxl import load_workbook
from openpyxl.utils import get_column_interval
from openpyxl.utils.cell import coordinate_from_string as cfs
import pandas as pd
from datetime import datetime, timedelta, date





wb          = load_workbook("C:\\Users\\l.haynes\\Desktop\\py_edit\\test.xlsx", data_only = True) # workbook (file)
df_dict2     = {}
ws2          = wb['Settings'] # worksheet (use interval for worksheet name)




## ws2["B5"] = "Vaughn Haynes"
wb.save("C:\\Users\\l.haynes\\Desktop\\py_edit\\test.xlsx")





print('Done!')

