from datetime import datetime, timedelta
import oracledb
import boto3
import io
import csv
import redshift_conn
from redshift_conn import conn,cursor,start_time
# Set up Oracle and S3 connection
bucket_name = 'etlworkflows'
table_name = 'offices'
etl_batch_date = redshift_conn.etl_batch_date
etl_batch_no = redshift_conn.etl_batch_no

try:
    oracledb.init_oracle_client()
    connection = oracledb.connect('g23konda/g23konda123@//54.224.209.13:1521/xe')
    s3 = boto3.client('s3',aws_access_key_id='AKIA4N7RVQEQUUAHHWTT',aws_secret_access_key='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')
    cursor1 = connection.cursor()
    query = f'''
            SELECT *          
            FROM {table_name}@konda_dblink_classicmodels
            WHERE to_char(update_timestamp, 'yyyy-mm-dd') >= '{etl_batch_date}'
            '''
    cursor1.execute(query)
    # Fetch column names and data
    col_names = [i[0] for i in cursor1.description]
    rows = cursor1.fetchall()
    csv_data = io.StringIO()
    csv_writer = csv.writer(csv_data)
    csv_writer.writerow(col_names)
    csv_writer.writerows(rows)
    # Encode the CSV data as bytes using UTF-8
    csv_bytes = csv_data.getvalue().encode('utf-8')
    # Upload the CSV data to S3
    s3_path = f'{table_name}/{etl_batch_date}/{table_name}.csv'
    s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_bytes)
    print(f'Uploaded {etl_batch_date}.{table_name} to S3')
except Exception as e:
    print(f'Failed to upload to S3: {str(e)}')
finally:
    connection.close()
    end_time = datetime.now()
    #Calculate the time taken
    time_taken = end_time - start_time
    cursor.execute(f"""
    INSERT INTO etl_metadata.batches(etl_batch_no, etl_batch_date, run_type, start_time, end_time, time_taken)
    VALUES({etl_batch_no}, '{etl_batch_date}', 'src_to_s3_offices', '{start_time}', '{end_time}', '{time_taken}')
    """)
    conn.commit()
    conn.close()