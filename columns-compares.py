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
percentage_Oracle_pgdb_count = count_Oracle_csv/count_PGDB_csv *100
print (' ############# to obtain total unique count ################')
unique_contract_ids = dfOracle['policy_number'].nunique()
print(' ############### to obtain total record for unique event date###############', "\n")
record_oracle_count = dfOracle.groupby('put your column').size().reset_index(name='Total_Records')

##############adding subset for missing columns##################3
dfPGDB_subset = dfPGDB[['Policy_number']]
dfOracle_subset = dfOracle[['policy_number']]
dfPGDB_subset.rename(columns= {'policy_number': 'policy_number_PGDB'}, inplace=True)
dfOracle_subset.rename(columns= {'policy_number': 'policy_number_PGDB'}, inplace=True)
print('dfPGDB_subset')

missing_in_PGDB = pd.merge(dfOracle, dfPGDB_subset, left_on = ['policy_nymber'], right_on= ['policy_number_PGDB'])
missing_in_PGDB1 = missing_in_PGDB[missing_in_PGDB['policy_number_PGDB'].isnull()]
