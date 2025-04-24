import boto3
import pymysql
import json
import os

s3 = boto3.client('s3')

# Database connection details
RDS_HOST = "fileprocessingdb.cfac6uqig0yl.ap-south-1.rds.amazonaws.com"
DB_USER = "admin"
DB_PASSWORD = "YourPassword"
DB_NAME = "fileprocessingdb"

def lambda_handler(event, context):
    try:
        print("Lambda function triggered!")
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            file_key = record['s3']['object']['key']

            print(f"Processing file: {file_key} from bucket: {bucket_name}")

            # Get file content from S3
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read().decode('utf-8')

            print(f"File content: {file_content}")

            # Try connecting to RDS
            try:
                connection = pymysql.connect(host=RDS_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
                print("✅ Successfully connected to RDS")
                
                with connection.cursor() as cursor:
                    insert_query = "INSERT INTO files (file_name, file_content) VALUES (%s, %s)"
                    cursor.execute(insert_query, (file_key, file_content))
                    connection.commit()
                
                print("✅ Data inserted successfully into RDS")
            
            except Exception as db_error:
                print(f"❌ Database connection failed: {str(db_error)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps(f"Database connection failed: {str(db_error)}")
                }

        return {
            'statusCode': 200,
            'body': json.dumps('File processed successfully!')
        }

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"File processing failed: {str(e)}")
        }
