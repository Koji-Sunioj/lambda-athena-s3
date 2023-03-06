import boto3
import time

def lambda_handler(event, context):
    client = boto3.client('athena')
    
    query_start = client.start_query_execution(
        QueryString = "SELECT count(distinct(request_ip)) as unique_ips\
            FROM cloudfront_logs \
            WHERE date = DATE '2023-03-06' and uri = '/';",
        QueryExecutionContext = {
            'Database':'default'
        },
        ResultConfiguration={
            'OutputLocation':'s3://testlog-koji/'
        }
    )
    
    query_id = query_start["QueryExecutionId"]
    time.sleep(15)
    results = client.get_query_results(QueryExecutionId = query_id)
    result = results["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
    print(result)
