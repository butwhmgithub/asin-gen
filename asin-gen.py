import boto3
import pandas as pd

def lambda_handler(event, context):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Specify the S3 bucket and key for the input CSV file
    bucket_name = 'XXXXXXXXXXXXXXXXXX'
    object_key = 'input.csv'

    obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = pd.read_csv(obj['Body'])

    # Concatenate the 'item id' and 'upc' columns to create the 'ASIN' column
    data['ASIN'] = data['item id'].astype(str) + data['upc'].astype(str)

    # Convert the DataFrame back to a CSV string
    output_csv = data.to_csv(index=False)

    # Specify the S3 bucket and key for the output CSV file
    output_bucket_name = 'XXXXXXXXXXXXXXXXXX'
    output_object_key = 'output/output.csv'

    # Upload the output CSV to S3
    s3.put_object(Body=output_csv, Bucket=output_bucket_name, Key=output_object_key)

    return {
        'statusCode': 200,
        'body': 'ASIN generation complete'
    }