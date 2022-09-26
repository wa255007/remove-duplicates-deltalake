import logging
import time
import boto3
import json
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('glue')
s3_client= boto3.client('s3')

def copy_script_to_s3(script_path,bucket, bucket_key):
    filepath= script_path
    filename=filepath.split("\\")[-1].replace(".py","")
    logger.info (f"file name read and deployed will be {filename}")
    key= f"{bucket_key}/{filename}.py"
    res= s3_client.put_object (Body=open(filepath,'rb'), Bucket=bucket,Key=key)
    bucket_key=f"s3://{bucket}/{key}"
    return filename, bucket_key

def create_glue_job(glue_job_name,role,script_location):
    logging.info(f"received parameters are {glue_job_name},{role} , {script_location}")
    job = client.create_job(Name=glue_job_name, Role=role,GlueVersion='3.0',
                          Command={'Name': 'glueetl',
                                   'ScriptLocation': script_location}
                            ,NumberOfWorkers=5
                            ,WorkerType='G.1X'
                           )
    logging.info(f"job created successfully")
    return job

def remove_duplicates_deltalake(glue_job_name,delta_lake_s3_path,primary_keys,filter_date,delta_lake_jar_path):
    logging.info(f"received parameters are {glue_job_name}")
    response = client.start_job_run(JobName = glue_job_name ,Arguments = {
                '--delta_lake_s3_path': delta_lake_s3_path,
                '--primary_keys': primary_keys,
                '--filter_date':filter_date,
                '--extra-jars': delta_lake_jar_path
              })
    logging.info(f"glue job started  {glue_job_name}")
    logging.info(f"glue job started  {response['JobRunId']}")
    logging.info(f"here is the response:  {response}")
    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        query_execution_response = client.get_job_run(JobName=glue_job_name,RunId=response['JobRunId'])
        query_status = query_execution_response['JobRun']['JobRunState']
        logging.info(f"Query status now is:  {query_status}")
        if query_status in ['FAILED', 'CANCELLED']:
            raise Exception(f'glue job   has {query_status}\n\n')
        time.sleep(10)
    return response

if __name__=="__main__":
    start_time = time.time()
    #load config file parameters
    with open("config.json") as f:
        map=f.read()
        config=json.loads(map)
    glue_job_name,s3_path_key = copy_script_to_s3(config["script_path"],config["bucket"], config["bucket_key"])
    response=create_glue_job(glue_job_name, config["role"], s3_path_key)
    res=remove_duplicates_deltalake(glue_job_name,config["delta_lake_s3_path"],config["primary_keys"],config["filter_date"],config["delta_lake_jar_path"])
    logging.info(f"Job completed successfully")
