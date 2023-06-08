import boto3
import pandas as pd
from io import StringIO
import psycopg2


# AWS credentials and Redshift details
# aws_access_key_id = 'YOUR_AWS_ACCESS_KEY_ID'
# aws_secret_access_key = 'YOUR_AWS_SECRET_ACCESS_KEY'
redshift_host = 'redshift-cluster-1.cnwomlrh6uek.us-east-1.redshift.amazonaws.com'
redshift_port = '5439'
redshift_database = 'dev'
redshift_user = 'awsuser'
redshift_password = 'Awsuser13'
redshift_table = 'product_table'

# S3 bucket and file details
input_bucket = 'myglue-etl-project'
input_file_key = 'input/product_data.csv'

# Read CSV file into a pandas DataFrame
s3 = boto3.client('s3')
obj = s3.get_object(Bucket=input_bucket, Key=input_file_key)
df = pd.read_csv(obj['Body'])

print(df)
print(df.shape[0])

print('connect to Redshift')
# Connect to Redshift
conn = psycopg2.connect(
    host=redshift_host,
    port=redshift_port,
    dbname=redshift_database,
    user=redshift_user,
    password=redshift_password
)
cur = conn.cursor()

# Generate the INSERT statement
columns = ", ".join(df.columns)
values = ", ".join(["%s"] * len(df.columns))
insert_query = f"INSERT INTO {redshift_table} ({columns}) VALUES ({values});"

print(insert_query)


# Convert DataFrame to a list of tuples
data = [tuple(row) for row in df.to_numpy()]

print(data)

# Execute the INSERT statement
cur.executemany(insert_query, data)
conn.commit()

# Close the database connection
cur.close()
conn.close()

