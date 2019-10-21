#Moving data to imaging_studies

import snowflake.connector
#from getpass import getpass
       
"Initialising Snowflake conncetions"
import credential as cred
con = snowflake.connector.connect(
user= cred.db_user,
password= cred.db_password,
account= cred.db_server) 
#  user= input("Please enter your username: "),
#  password= getpass("Please enter your password: "),
#  account='saggezzapartner.us-east-1'

cur = con.cursor()
print("successfully established the connection")

#The below code is choosing the relevant role, warehouse, database and schema in the snowflake
cur.execute("USE ROLE PATIENT360")
cur.execute("USE WAREHOUSE PATIENT360_WH")
cur.execute("USE SCHEMA PATIENT360_DB.TEST")

print("Selected appropriate role, warehouse, schema and database")

#Moving data to staging area

cur.execute("""
CREATE OR REPLACE STAGE PATIENT360_DB.TEST.IMAGING_STUDIES_STAGING
file_format = PATIENT360_DB.TEST.FF_P360
""")

cur.execute("PUT file://my_data/imaging_studies.csv @PATIENT360_DB.TEST.IMAGING_STUDIES_STAGING")

#Moving data from staging to source table with all correct data types

cur.execute("""
COPY INTO PATIENT360_DB.TEST.IMAGING_STUDIES
(
ID,
DATE,
PATIENT,
ENCOUNTER,
BODYSITECODE,
BODYSITEDESCRIPTION,
MODALITYCODE,
MODALITYDESCRIPTION,
SOPCODE,
SOPDESCRIPTION
)
FROM
( SELECT  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
FROM @PATIENT360_DB.TEST.IMAGING_STUDIES_STAGING
)on_error = 'Continue';
""")

print("imaging_studies data moved to table")

con.close()
print("succesfully closed the connection")