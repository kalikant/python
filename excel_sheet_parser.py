import numpy as np
import pandas as pd
df_isd = pd.read_excel('automation\\Interface Spec v1.0.xlsx')
xls = pd.ExcelFile('automation\\Interface Spec v1.0.xlsx')

# to read all sheets to a map
sheet_to_df_map = {}
df_sheets = []
for sheet_name in xls.sheet_names:
    sheet_to_df_map[sheet_name] = xls.parse(sheet_name)
    df_sheets=pd.read_excel(xls,sheet_name)
    print df_sheets
