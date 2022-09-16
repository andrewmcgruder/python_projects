import pyodbc
import boto3 as b
import csv
import os

#Hissy time
print("Opening list of sudds tables...")

#The csv file reference below used had the following fields:
#SOURCE_TABLE_NAME
#SOURCE_NAME
#SOURCE_TABLE_SCHEMA
#SOURCE_SQL_QUERY
#TARGET_TABLE_NAME
#TARGET_TABLE_SCHEMA
#SOURCE_TABLE_DATABASE

sudds_list = open(r'local_file_of_tables_needed_to_copy.csv')
print("Done. Reading components...")

csvreader = csv.reader(sudds_list)
header = next(csvreader)

print("Setting up variables...")
SOURCE_TABLE_NAME_INDEX = header.index("SOURCE_TABLE_NAME")
SOURCE_NAME_INDEX = header.index("SOURCE_NAME")
SOURCE_TABLE_SCHEMA_INDEX = header.index("SOURCE_TABLE_SCHEMA")
SOURCE_SQL_QUERY_INDEX = header.index("SOURCE_SQL_QUERY")
TARGET_TABLE_NAME_INDEX = header.index("TARGET_TABLE_NAME")
TARGET_TABLE_SCHEMA_INDEX = header.index("TARGET_TABLE_SCHEMA")
SOURCE_TABLE_DATABASE_INDEX = header.index("SOURCE_TABLE_DATABASE")
print("Done.")

# Loop through the lines in the file.
print("Setting up variable list...")
# Make an empty list
sudds_vars = []

#iterates through each row in the list to issue queries that subsequently make it to S3.
for row in csvreader:
    source_table = row[SOURCE_TABLE_NAME_INDEX]
    source_database = row[SOURCE_NAME_INDEX]
    source_schema = row[SOURCE_TABLE_SCHEMA_INDEX]
    source_sql = row[SOURCE_SQL_QUERY_INDEX]
    target_table = row[TARGET_TABLE_NAME_INDEX]
    target_table_schema = row[TARGET_TABLE_SCHEMA_INDEX]
    sudds_vars.append([source_table,source_database,source_schema,source_sql,target_table,target_table_schema])

    s3_resource = b.resource('s3')
    Bucketname = '[YOUR BUCKET NAME]'

    print("Setting up connection to SUDDS...")
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=YOUR_SERVER_NAME;"
                          "Database="+source_database+";"
                          "Trusted_Connection=no;"
    					  "uid=username;password") #I know this is not ideal - this should really be done via AWS secrets. To improve...

    print("Complete.")

    cursor = cnxn.cursor()

    print("Executing query against " + source_table)
    rows = cursor.execute(source_sql)
    filename = ("C:\\directory_of_source_csv_with_tables_to_extract\\" + source_table + ".csv")

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])  # column headers
        for row in rows:
            writer.writerow(row)

    print("File written successfully. Filename: " + target_table +".csv")
    sts_client = b.client('sts')
    account_id =sts_client.get_caller_identity().get('Account')

    #print("The current account ID is: " + account_id)

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    assumed_role_object=sts_client.assume_role(
        RoleArn="arn:aws:iam::aws_account_number:role/aws_role", #replace aws_account_number and aws_role with your AWS credentials here
        RoleSessionName="AssumeRoleSession1"
    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
    credentials=assumed_role_object['Credentials']

    s3_resource=b.resource(
       's3',
       aws_access_key_id=credentials['AccessKeyId'],
       aws_secret_access_key=credentials['SecretAccessKey'],
       aws_session_token=credentials['SessionToken'],
    )

    #print("\n\n.....Available buckets.....")
    # Use the Amazon S3 resource object that is now configured with the
    # credentials to access your S3 buckets.
    #for bucket in s3_resource.buckets.all():
    #   print(bucket.name)
    #print("..........................\n\n")

    bucket1 = s3_resource.Bucket('s3_bucket_name') #your s3 bucket target
    targetlanding = 'subsequent_path_to_where_you_want_files_to_go' #i.e., /files/target/
    print("Bucket name: " + bucket1.name)

    print("Uploading "+ target_table + '.csv' + " to S3 bucket...")
    s3_resource.Bucket(bucket1.name).upload_file(filename, targetlanding + source_database + '/' + target_table +'.csv')
    print("Done.")

    print("Removing source file " + target_table + '.csv')
    os.remove(filename)
    print("Done.\n\n")
