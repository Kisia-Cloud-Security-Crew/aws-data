import pymysql
import boto3
import uuid
import time
import os
import mimetypes
import json

# 환경 변수에서 RDS MySQL 연결 정보 가져오기
rds_host = os.getenv("RDS_HOST")  # RDS 엔드포인트 호스트 이름
db_name = os.getenv("DB_NAME")  # 데이터베이스 이름
username = os.getenv("DB_USER")  # 사용자 이름
password = os.getenv("DB_PASS")  # 비밀번호
port = int(os.getenv("DB_PORT", 3306))  # 포트 (기본 3306)
DBinstanceIdentifier = os.getenv("DB_INSTANCE_ID")  # RDS 인스턴스 식별자 (예: "lee")

# boto3로 RDS 메타데이터 가져오기
def get_rds_metadata():
    client = boto3.client('rds', region_name='ap-northeast-2')  # AWS 리전 설정
    try:
        response = client.describe_db_instances(DBInstanceIdentifier=DBinstanceIdentifier)
        instance_info = response['DBInstances'][0]
        return instance_info
    except Exception as e:
        print(f"Error retrieving RDS metadata: {e}")
        return None

# MySQL 연결 설정
def connect_to_rds():
    try:
        connection = pymysql.connect(
            host=rds_host,
            user=username,
            password=password,
            database=db_name,
            port=port,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Connection to RDS MySQL instance established successfully.")
        return connection
    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# 파일 접근 권한 및 유형 정보를 가져오는 함수
def get_file_metadata(file_path):
    try:
        file_metadata = {
            'path': file_path,
            'is_readable': os.access(file_path, os.R_OK),
            'is_writable': os.access(file_path, os.W_OK),
            'is_executable': os.access(file_path, os.X_OK),
            'mime_type': mimetypes.guess_type(file_path)[0]
        }
        file_metadata['data_type'] = 'structured' if file_metadata['mime_type'] and 'text' in file_metadata['mime_type'] else 'unstructured'
        return file_metadata
    except Exception as e:
        print(f"Error retrieving file metadata: {e}")
        return None

# MySQL 시스템 설정 변수 가져오기
def get_mysql_config(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW VARIABLES;")
            config_data = cursor.fetchall()
            return config_data
    except pymysql.MySQLError as e:
        print(f"Error retrieving MySQL config: {e}")
        return None

# 데이터 전처리 함수
def preprocess_data(rds_metadata, mysql_config, file_metadata):
    filtered_rds_metadata = {k: v for k, v in rds_metadata.items() if k in ['DBInstanceIdentifier', 'DBInstanceStatus', 'Engine', 'Endpoint']}
    filtered_mysql_config = [config for config in mysql_config if config['Variable_name'] in ['max_connections', 'innodb_buffer_pool_size']]
    sensitive_info = ["password", "secret"]
    filtered_file_metadata = {k: v for k, v in file_metadata.items() if k not in sensitive_info}
    return filtered_rds_metadata, filtered_mysql_config, filtered_file_metadata

# 메타데이터 및 설정 값을 MySQL에 업로드하는 함수
def upload_metadata_and_config(connection, rds_metadata, mysql_config, file_metadata):
    try:
        with connection.cursor() as cursor:
            # RDS 메타데이터 업로드
            cursor.execute(
                "INSERT INTO rds_metadata_table (id, instance_info, timestamp) VALUES (%s, %s, %s)",
                (str(uuid.uuid4()), json.dumps(rds_metadata), int(time.time()))
            )

            # MySQL 설정 값 업로드
            cursor.execute(
                "INSERT INTO mysql_config_table (id, config_data, timestamp) VALUES (%s, %s, %s)",
                (str(uuid.uuid4()), json.dumps(mysql_config), int(time.time()))
            )

            # 파일 메타데이터 업로드
            cursor.execute(
                "INSERT INTO file_metadata_table (id, file_path, is_readable, is_writable, is_executable, mime_type, data_type, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    str(uuid.uuid4()),
                    file_metadata['path'],
                    file_metadata['is_readable'],
                    file_metadata['is_writable'],
                    file_metadata['is_executable'],
                    file_metadata['mime_type'],
                    file_metadata['data_type'],
                    int(time.time())
                )
            )
            connection.commit()
            print("Metadata, config, and file info uploaded to MySQL successfully.")
    except pymysql.MySQLError as e:
        connection.rollback()
        print(f"Error uploading data to MySQL: {e}")

# 메타데이터 수집 및 전처리 후 업로드
rds_metadata = get_rds_metadata()
if rds_metadata:
    connection = connect_to_rds()
    if connection:
        mysql_config = get_mysql_config(connection)
        if mysql_config:
            file_path = "/path/to/your/config/file.conf"  # 파일 경로 설정 필요
            file_metadata = get_file_metadata(file_path)
            if file_metadata:
                processed_rds_metadata, processed_mysql_config, processed_file_metadata = preprocess_data(rds_metadata, mysql_config, file_metadata)
                upload_metadata_and_config(connection, processed_rds_metadata, processed_mysql_config, processed_file_metadata)
        connection.close()
