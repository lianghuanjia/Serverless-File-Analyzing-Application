import uuid

import boto3
import json

from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
sqs = boto3.client('sqs')
rekognition = boto3.client('rekognition')

dynamodb = boto3.resource('dynamodb')
queue_url = 'https://sqs.us-east-1.amazonaws.com/403357069823/WarehouseSQS'
table = dynamodb.Table('ProcessedPDFTextTable')


def lambda_handler(event, context):
    if event['Records'][0]['eventSource'] == 'aws:s3':
        # Handle S3 event
        try:
            bucket = event['Records'][0]['s3']['bucket']['name']
            filename = event['Records'][0]['s3']['object']['key']
            message_body = json.dumps({"bucket": bucket, "filename": filename})         

            response = sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
            print(response)
            return {
                'statusCode': 200,
                'body': json.dumps('Inserted successfully.')
            }
        except Exception as e:
            return {
                'statusCode': 400,
                'body': json.dumps(str(e))
            }
        
    elif event['Records'][0]['eventSource'] == 'aws:sqs':
        # Handle SQS event
        message = json.loads(event['Records'][0]['body'])
        bucket = message['bucket']
        filename = message['filename']
        print("bucket: " + bucket)
        print("fileName: " + filename)
        rekognition_response = rekognition.detect_text(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': filename
                }
            }
        )
        detected_text = [text_detection['DetectedText'] for text_detection in rekognition_response['TextDetections']]
        try:
            table.put_item(
                Item={
                    'file_name': filename,
                    'detected_labels': json.dumps(detected_text),
                }
            )
            return {
                'statusCode': 200,
                'body': json.dumps('Insert into DB successfully')
            }
        except Exception as e:
            return {
                'statusCode': 400,
                'body': json.dumps(str(e))
            }
        