import json
import boto3
import datetime
import gzip
import pandas as pd
from datetime import datetime, timezone

client = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = "ironpondstack-logbucketcc3b17e8-1t5bduk77ymwx"
    time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    s3_paginator = client.get_paginator('list_objects_v2')
    s3_iterator = s3_paginator.paginate(Bucket=bucket_name)
    filter = "Contents[?to_string(LastModified)>='\"{}\"'].Key".format(time)
    filtered_iterator = s3_iterator.search(filter)
    
    headers = None
    lists = []
    
    for n,object in enumerate(filtered_iterator):
        response =  client.get_object(
            Bucket=bucket_name,
            Key=object
        )
        with gzip.open(response["Body"],'r') as fin:        
            content = fin.read().decode("utf-8").split("\n")
            for con in content:
                columns = con.split("\t")
                if len(columns) > 1 and columns[7] == "/":
                   lists.append(columns)
                elif n == 0 and "Fields" in columns[0]:
                    headers = columns[0].replace("#Fields: ","").split(" ")
    
    raw_frame = pd.DataFrame(data=lists,columns=headers)
    unique_by_ip= raw_frame.drop_duplicates(subset=['c-ip'])
