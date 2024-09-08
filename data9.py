import boto3
import json
import time
from datetime import datetime, timezone
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# AWS 리소스 초기화
s3 = boto3.resource('s3', region_name='ap-northeast-2')
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
bucket_name = 'lsclsc'  # S3 버킷 이름
table_name = 'lee'  # DynamoDB 테이블 이름

# 로그 파일 경로 설정
log_file_path = '/SystemRoot/System32/winevt/Logs'  # 로그 파일의 경로를 입력하세요

def collect_data():
    """로그 파일에서 데이터를 수집하는 함수"""
    try:
        with open(log_file_path, 'r') as file:
            logs = file.readlines()  # 로그 파일의 모든 줄을 읽음
            data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "logs": logs
            }
        return data
    except FileNotFoundError:
        print(f"Log file at {log_file_path} not found.")
        return None

def store_data_in_s3(data):
    """S3에 데이터 저장 함수"""
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    file_name = f'log_data_{timestamp}.json'
    try:
        s3_object = s3.Object(bucket_name, file_name)
        s3_object.put(Body=json.dumps(data))
        print(f'Log data stored in S3 with file name: {file_name}')
    except NoCredentialsError:
        print("AWS credentials not found. Please configure AWS credentials.")
    except PartialCredentialsError:
        print("AWS credentials are incomplete. Please check the configuration.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            print("Access denied. Check your IAM permissions.")
        elif error_code == 'NoSuchBucket':
            print(f"The bucket '{bucket_name}' does not exist.")
        else:
            print(f"Unexpected error occurred when storing data in S3: {e}")

def store_data_in_dynamodb(data):
    """DynamoDB에 데이터 저장 함수"""
    table = dynamodb.Table(table_name)
    timestamp = datetime.now(timezone.utc).isoformat()
    try:
        response = table.put_item(
            Item={
                'qwer': timestamp,  # 파티션 키
                'asdf': timestamp,  # 정렬 키
                'log_data': json.dumps(data)  # 로그 데이터를 JSON 문자열로 저장
            }
        )
        print(f'Log data stored in DynamoDB with timestamp: {timestamp}')
    except NoCredentialsError:
        print("AWS credentials not found. Please configure AWS credentials.")
    except PartialCredentialsError:
        print("AWS credentials are incomplete. Please check the configuration.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFoundException':
            print(f"The table '{table_name}' does not exist.")
        elif error_code == 'ProvisionedThroughputExceededException':
            print(f"Provisioned throughput for the table '{table_name}' has been exceeded.")
        else:
            print(f"Unexpected error occurred when storing data in DynamoDB: {e}")

if __name__ == '__main__':
    while True:
        data = collect_data()
        if data:
            store_data_in_s3(data)
            store_data_in_dynamodb(data)
        time.sleep(60)
