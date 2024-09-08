import boto3
import json

s3 = boto3.client('s3')
bucket_name = 'lsclsc'
key = 'Setup.evtx'

def lambda_handler(event, context):
    try:
        # S3에서 파일 가져오기
        response = s3.get_object(Bucket=bucket_name, Key=key)
        file_content = response['Body'].read().decode('utf-8')

        return {
            'statusCode': 200,
            'body': json.dumps(file_content),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error fetching data: {e}"),
            'headers': {
                'Content-Type': 'application/json'
            }
        }