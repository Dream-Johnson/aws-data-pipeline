import boto3
import json
import os

def lambda_handler(event, context):
    client = boto3.client('stepfunctions', region_name='ap-south-2')
    
    # Get the file that triggered this Lambda
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(f"New file detected: s3://{bucket}/{key}")
    print("Starting DE pipeline...")
    
    # Start Step Functions execution
    response = client.start_execution(
        stateMachineArn=os.environ['STATE_MACHINE_ARN'],
        input=json.dumps({
            "triggered_by": f"s3://{bucket}/{key}"
        })
    )
    
    print(f"Pipeline started: {response['executionArn']}")
    
    return {
        'statusCode': 200,
        'body': f"Pipeline triggered by s3://{bucket}/{key}"
    }