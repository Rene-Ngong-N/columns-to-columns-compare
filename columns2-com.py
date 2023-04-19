import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import configparser
from io import StringIO
import uuid
import datetime
Dhub = f2.toPandas()
Pgdb = f1.toPandas()
#restrict to policu numbers in Dhub for comparison  
Pgdb = pd.merge(Pgdb, Dhub[['policy_number']], on = 'policy_number', how = 'inner')
pgdb_col_names = ['policy_number']
for col in list(Dhub.columns):
    if col != 'policy_number':
        pgdb_col_names.append(col+'_pgdb')

Pgdb.columns = pgdb_col_names
columns_to_compare = ['ProductID', 'ProductCode', 'Surrender_Charge_Period',
       'BenefitID', 'Benefit_DESC', 'Commission_Option', 'Contract_Status',
       'Issuing_Company', 'Market_Type', 'Distribution_Channel', 'Firm_Name',
       'AgentID', 'AgentName', 'App_Sign_State', 'App_Sign_Date', 'Annuitant_Role', 'Annuitant_Name', 'Annuitant_Age',
       'CoAnnuitant_Role', 'CoAnnuitant_Name', 'CoAnnuitant_Age', 'Owner_Role', 'Owner_Name',
       'Owner_Age', 'Joint_Owner_Role', 'Joint_Owner_Name', 'Joint_Owner_Age',
       'ExpectedPrem', 'PlannedPrem', 'DTCCDocCntl', 'APPTYPE', 'Is_Index_Start_Deferred', 'Consent_Indicator', 'Consent_Type', 'Consent_Date', 'Novated_Indicator', 'Owner_Sign_Date', 'Annuitant_Sign_Date', 'Trustee_Sign_Date']

today = str(date.today()).replace('-','')
cmp = {}
for col in columns_to_compare:
    Dhub_subset = Dhub[['policy_number', col]].drop_duplicates()
    Pgdb_subset = Pgdb[['policy_number', col+'_pgdb']].drop_duplicates()
    Pgdb_subset_grp = Pgdb_subset.groupby('policy_number').count().reset_index(inplace=False)
    Pgdb_subset_grp.rename(columns={col+'_pgdb': 'duplicates'}, inplace=True)
    Pgdb_subset_grp = Pgdb_subset_grp[Pgdb_subset_grp['duplicates']>1]
    Dhub_subset_new = Dhub_subset.fillna('')
    Pgdb_subset_new = Pgdb_subset.replace(' ', '')
    Pgdb_subset_final = Pgdb_subset_new.fillna('')
    join = pd.merge(Dhub_subset_new, Pgdb_subset_final, on ='policy_number')
    # print(join)
    join['matched'] = (join[col]==join[col+'_pgdb']).astype(int)
    join_matched = join[join['matched']==1]
    cmp[col] = join_matched.shape[0]/join.shape[0]
    join = pd.merge(join, Pgdb_subset_grp, on = 'policy_number', how = 'left')
    join['duplicates'] = join['duplicates'].fillna(0)
    
    file_name = 'file_path'+today+ '/' + col+'_'+today+'.csv'
    join.to_csv(file_name)
    print(file_name)
    print(join_matched.shape[0]/join.shape[0])
    print('\n')
