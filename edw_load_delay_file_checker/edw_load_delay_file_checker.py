import boto3
from pandas.core.frame import DataFrame
from pandas.io.formats.format import DataFrameFormatter
import redshift_connector
import pandas
import json

#might need to automate how to install redshift_connector as a prerequisite above

conn = redshift_connector.connect(
     host='blahblah.redshift.amazonaws.com', #redshift target URL
     database='databasename',
     user='username',
     password='password' #again, not ideal. Was testing this locally - it should really use AWS secrets.
  )

#connects to a metadata table that gets a new row once the EDW load completes. This implies that if a new row isn't inserted, then we know something is wrong...
cursor = conn.cursor()
cursor.execute("select case when date(current_date) = data_load_date then 1 else 0 end as res from audit.data_load_schedule order by data_load_schedule.data_load_date desc limit 1")
df: pandas.DataFrame = cursor.fetch_dataframe()
val=df.iat[0,0]

if(val == 0):
  sns = boto3.client('sns')
  # Publish a simple message to the specified SNS topic
  response = sns.publish(
      TopicArn='arn:aws:sns:aws_region:aws_account_number:topic_name', #replace this with your account number and an SNS topic to send SMS messages to in case of failure.  
      Message='Warning: The EDW load appears to be delayed. Please investigate.',   
  )
else:
  (
  #do nothing
  )