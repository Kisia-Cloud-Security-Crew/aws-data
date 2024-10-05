import pymysql
import boto3
import uuid
import time

# RDS MySQL 연결 정보
rds_host = "your-rds-endpoint.rds.amazonaws.com"  # RDS 엔드포인트
db_name = "your_database_name"  # 데이터베이스 이름
username = "your_username"  # MySQL 사용자 이름
password = "your_password"  # MySQL 비밀번호
port = 3306  # 기본 MySQL 포트

# boto3로 RDS 메타데이터 가져오기 (AWS RDS 인스턴스 상태 및 정보)
def get_rds_metadata():
    client = boto3.client('rds', region_name='ap-northeast-2')  # 필요한 리전으로 변경
    try:
        response = client.describe_db_instances(DBInstanceIdentifier="your-instance-id")
        instance_info = response['DBInstances'][0]  # 첫 번째 인스턴스 정보
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

# MySQL 시스템 설정 변수 및 초기 설정 값 가져오기
def get_mysql_config(connection):
    try:
        with connection.cursor() as cursor:
            # MySQL 설정 변수 조회
            sql = "SHOW VARIABLES;"
            cursor.execute(sql)
            config_data = cursor.fetchall()  # 전체 시스템 변수와 설정 값
            return config_data
    except pymysql.MySQLError as e:
        print(f"Error retrieving MySQL config: {e}")
        return None

# 수집한 메타데이터와 설정 값을 MySQL에 업로드하는 함수
def upload_metadata_and_config(connection, rds_metadata, mysql_config):
    try:
        with connection.cursor() as cursor:
            # RDS 메타데이터 업로드
            sql_metadata = """
            INSERT INTO rds_metadata_table (id, instance_info, timestamp) 
            VALUES (%s, %s, %s)
            """
            cursor.execute(sql_metadata, (str(uuid.uuid4()), str(rds_metadata), int(time.time())))

            # MySQL 설정 값 업로드
            sql_config = """
            INSERT INTO mysql_config_table (id, config_data, timestamp)
            VALUES (%s, %s, %s)
            """
            cursor.execute(sql_config, (str(uuid.uuid4()), str(mysql_config), int(time.time())))
            
            connection.commit()
            print("Metadata and config uploaded to MySQL successfully.")
    except pymysql.MySQLError as e:
        print(f"Error uploading data to MySQL: {e}")

# MySQL에 연결 후 메타데이터 및 설정 가져와 업로드
rds_metadata = get_rds_metadata()

if rds_metadata:
    connection = connect_to_rds()
    if connection:
        mysql_config = get_mysql_config(connection)
        if mysql_config:
            upload_metadata_and_config(connection, rds_metadata, mysql_config)
        connection.close()
