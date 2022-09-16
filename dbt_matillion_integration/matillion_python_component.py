import os
from datetime import date
import boto3
import base64
from botocore.exceptions import ClientError

#Andrew McGruder 2019-12-19
#Sets up dbt files to create models and run regression tests.


#set up some handy functions that we'll use later to parse files (thanks StackOverflow)
#################################################################
def between(value, a, b):
    # Find and validate before-part.
    pos_a = value.find(a)
    if pos_a == -1: return ""
    # Find and validate after part.
    pos_b = value.rfind(b)
    if pos_b == -1: return ""
    # Return middle part.
    adjusted_pos_a = pos_a + len(a)
    if adjusted_pos_a >= pos_b: return ""
    return value[adjusted_pos_a:pos_b]
  
def after(value, a):
    # Find and validate first part.
    pos_a = value.rfind(a)
    if pos_a == -1: return ""
    # Returns chars after the found string.
    adjusted_pos_a = pos_a + len(a)
    if adjusted_pos_a >= len(value): return ""
    return value[adjusted_pos_a:]

#Get stored credentials from secret. For some reason trying to use 
#the json library didn't like how AWS spit out the JSON output
#of the secret. This isn't a huge deal because we can simply store
#the output as a string and parse out what we need.
#################################################################
secret_name = "secret_name"
region_name = "region_name"

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client(
  service_name='secretsmanager',
  region_name=region_name
)

get_secret_value_response = client.get_secret_value(
  SecretId=secret_name
)
jsonstr=str(get_secret_value_response)
print(jsonstr)
passd=between(jsonstr,"\"password\":\"","\",\"engine\":\"")

#Change the directory to DBT and set the location of profiles.yml
#then build, test, and write the output to a file
#################################################################
root = os.path.abspath(os.sep)
load_date = str(date.today())
os.chdir(root + '/home/dbt')

#Create YAML file for dbt to read. This way is not ideal, 
#but using yml libraries in py makes weird dashes that dbt 
#doesn't like
#################################################################

line1="source_db:"
line2="  target: target_env_name"
line3="  outputs:"
line4="    dev:"
line5="      type: dbms_type"
line6="      host: blahlbalhlbalh.redshift.amazonaws.com"
line7="      user: username"
line8="      pass: " + passd
line9="      port: 5439"
line10="      dbname: databasename"
line11="      schema: dbt_test_schema"
line12="      threads: 4"
with open('profiles.yml','w') as out:
    out.write('{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format(line1,line2,line3,line4,line5,line6,line7,line8,line9,line10,line11,line12))


#Change directory to where the dbt project lives and then execute all of the tests. 
#The three os.system commands need to be combined as they depend on each other in succession for them to work
#################################################################
os.chdir(root + '/home/dbt/folder_name_on_ec2_instance_where_dbt_results_are_stored')
#> os.devnull
os.system('export DBT_PROFILES_DIR=/home/dbt && dbt run  && dbt test > test_results/regression_results_' + load_date + '.txt')
#################################################################


#Get the last line from the regression results. Then analyze the output
#################################################################
with open("test_results/regression_results_" + load_date + ".txt", "rb") as f:
    first = f.readline()        # Read the first line.
    f.seek(-2, os.SEEK_END)     # Jump to the second last byte.
    while f.read(1) != b"\n":   # Until EOL is found...
        f.seek(-2, os.SEEK_CUR) # ...jump back the read byte plus one more.
    last = str(f.readline())         # Read last line.


  
#look for no errors in the final output. If it doesn't exist, 
#that indicates that one or more errors have occurred.
no_errors="ERROR=0"
if no_errors in last:
	context.updateVariable('regression_pass_fail','Pass')
	print("REGRESSION PASS")
else:
	context.updateVariable('regression_pass_fail','Fail')
	print("REGRESSION FAIL")
  
  
passcnt=int(between(last,"PASS=","W"))
print("Pass count is " + str(passcnt))

errorcnt=int(between(last,"ERROR=","S"))
print("Error count is " + str(errorcnt))

totalcnt=int(between(last,"TOTAL=","\\n"))
print("Total count is " + str(totalcnt))

failurepct= round(100 * float(errorcnt)/float(totalcnt),2)
print("Failure percentage: " + str(failurepct) + "\n")

#Update Matillion variables to be kicked through to the next step
#The failure count will determine what kind of SNS message we send 
#out
context.updateVariable('regression_pass', passcnt)
context.updateVariable('regression_fail', errorcnt)
context.updateVariable('regression_total', totalcnt)
context.updateVariable('regression_failure_percentage', failurepct)

with open ("test_results/regression_results_" + load_date + ".txt", "r") as regression_results:
	regression_output=regression_results.read()
formatted_regression_output=regression_output.replace('\\n', '\n')
print(formatted_regression_output)
      
context.updateVariable('regression_results', formatted_regression_output)

#delete source yaml file - we don't want this sitting on the server indefinitely
#because it stores the redshift credentials in there. 
os.remove(root + '/home/dbt/profiles.yml')
