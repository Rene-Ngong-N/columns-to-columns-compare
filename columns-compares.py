from datetime import date
import datetime
import pandas as pd
import openpyxl
from openpyxl import load_workbook
import csv

file_path = "differences.xls"
dfOracle = pd.read_csv(r'C:\user\Desktop')
dfPGDB = pd.read_csv(r'C:\user\Desktop')
Oracle_key_cols = dict.fromkeys(zip(dfOracle['policy_number']))
PGDB_key_cols = dict.fromkeys(zip(dfPGDB['Policy_number']))
from collections import defaultdict
missing_dfPGDB_in_dfOracle = defaultdict(list)
missing_dfOracle_in_dfPGDB = defaultdict(list)
for indx, val in enumerate(Oracle_key_cols.keys()):
    if val not in PGDB_key_cols:
        for key, val in dfOracle.items():
            missing_dfOracle_in_dfPGDB[key].append(dfOracle[key][indx])
            
for indx, val in enumerate(PGDB_key_cols.keys()):
    if val not in Oracle_key_cols:
        for key, val in dfPGDB.items():
            missing_dfPGDB_in_dfOracle[key].append(dfOracle[key][indx])
            
Oracle_csv = pd.DataFrame(missing_dfOracle_in_dfPGDB)
PGDB_csv = pd.DataFrame(missing_dfPGDB_in_dfOracle)
Oracle_csv.to_csv('Oracle_v1.csv', index=False)
PGDB_csv.to_csv('Postgres_v1.csv', index=False)
print ("starting Validation............................................", "\n")
print(" ######################## Total record count from Oracle database #########################", "\n")
count_Oracle_csv = len(dfOracle)
count_PGDB_csv = len(dfPGDB)
percentage_Oracle_pgdb_count = count_Oracle_csv/count_PGDB_csv * 100

