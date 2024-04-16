import json
from datetime import datetime
import boto3

def transform(data):
    start_date = datetime.strptime(data['startDate'], "%Y-%m-%d")
    end_date = datetime.strptime(data['endDate'], "%Y-%m-%d")
    booking_duration = (end_date - start_date).days
    data['booking_duration'] = booking_duration
    return data

def lambda_handler(event, context):
    data_tf = []

    try:
        print("Starting Transformation Process...")
        try:
            list_records = json.loads(event['Records'][0]['body'])
        except:
            list_records = json.loads(event[0]['body'])
        print(f"Processing records...")
            
        for record in list_records:
            record_tf = transform(record)
            if record_tf['booking_duration'] > 1:
                data_tf.append(record_tf)

        print(f"Transformed data.")
        return {
            'statusCode': 200,
            'body': json.dumps(data_tf)
        }
    
    except Exception as err:
        print(err)
        print('Data transformation - FAILED!')
'''
def lambda_handler(event, context):
    try:
        print('Event: ', event)
        print('context: ', context)
        print(event)
        message =  json.loads(event[0]['body'])
        print(message)
        if (message['startDate'] == message['endDate']):
            message = {}
        return {
            'message': message
        }
    except Exception as e:
        return {
            'Error message': str(e)
        }
'''
