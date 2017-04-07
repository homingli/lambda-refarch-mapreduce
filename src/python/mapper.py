'''
Python mapper function

* Copyright 2016, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License. 
'''

import boto3
import json
import random
import resource
import StringIO
import time

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# constants
TASK_MAPPER_PREFIX = "task/mapper/";

def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

def lambda_handler(event, context):
    
    start_time = time.time()

    job_bucket = event['jobBucket']
    src_bucket = event['bucket']
    src_keys = event['keys']
    job_id = event['jobId']
    mapper_id = event['mapperId']
   
    # aggr 
    totals={}
    line_count = 0
    err = ''

    # INPUT CSV => OUTPUT JSON

    # Download and process all keys
    for key in src_keys:
        response = s3_client.get_object(Bucket=src_bucket,Key=key)
        contents = response['Body'].read()
        
        for line in contents.split('\n')[:-1]:
            line_count +=1

            try:
                data = line.strip().split(',')
                status = data[-1]
                if status == "OK":
                    interface_id = data[2]
                    srcIp = data[3]
                    gbytes = float(data[9])/(1024*1024*1024)
                    if srcIp not in totals:
                        totals[srcIp] = gbytes
                    else:
                        totals[srcIp] += gbytes
                    #if interface_id not in totals:
                    #    totals[interface_id] = gbytes
                    #else:
                    #    totals[interface_id] += gbytes

            except Exception, e:
                print e
                err += '%s' % e

    time_in_secs = (time.time() - start_time)
    #timeTaken = time_in_secs * 1000000000 # in 10^9 
    #s3DownloadTime = 0
    #totalProcessingTime = 0 
    pret = [len(src_keys), line_count, time_in_secs, err]
    mapper_fname = "%s/%s%s" % (job_id, TASK_MAPPER_PREFIX, mapper_id) 
    metadata = {
                    "linecount":  '%s' % line_count,
                    "processingtime": '%s' % time_in_secs,
                    "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
               }

    print "metadata", metadata
    write_to_s3(job_bucket, mapper_fname, json.dumps(totals), metadata)
    return pret

'''
ev = {
   "bucket": "-useast-1", 
   "keys": ["key.sample"],
   "jobId": "pyjob",
   "mapperId": 1,
   "jobBucket": "-useast-1"
   }
lambda_handler(ev, {});
'''
