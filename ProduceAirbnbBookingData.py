import json
import boto3
import random
from datetime import datetime, timedelta

# Generate random booking ID, user ID, and property ID
def generate_random_id(length):
    return ''.join(random.choices('0123456789', k=length))

# Generate random location (city, country)
def generate_random_location():
    locations = [
        ('New York', 'USA'), ('Los Angeles', 'USA'), ('Chicago', 'USA'), ('Houston', 'USA'), ('Phoenix', 'USA'),
        ('Toronto', 'Canada'), ('Vancouver', 'Canada'), ('Montreal', 'Canada'), ('Calgary', 'Canada'), ('Ottawa', 'Canada'),
        ('London', 'UK'), ('Manchester', 'UK'), ('Birmingham', 'UK'), ('Glasgow', 'UK'), ('Liverpool', 'UK'),
        ('Sydney', 'Australia'), ('Melbourne', 'Australia'), ('Brisbane', 'Australia'), ('Perth', 'Australia'), ('Adelaide', 'Australia'),
        ('Berlin', 'Germany'), ('Munich', 'Germany'), ('Hamburg', 'Germany'), ('Frankfurt', 'Germany'), ('Cologne', 'Germany'),
        ('Paris', 'France'), ('Marseille', 'France'), ('Lyon', 'France'), ('Toulouse', 'France'), ('Nice', 'France'),
        ('Rome', 'Italy'), ('Milan', 'Italy'), ('Naples', 'Italy'), ('Turin', 'Italy'), ('Palermo', 'Italy'),
        ('Madrid', 'Spain'), ('Barcelona', 'Spain'), ('Valencia', 'Spain'), ('Seville', 'Spain'), ('Bilbao', 'Spain'),
        ('Tokyo', 'Japan'), ('Osaka', 'Japan'), ('Nagoya', 'Japan'), ('Sapporo', 'Japan'), ('Fukuoka', 'Japan'),
        ('Shanghai', 'China'), ('Beijing', 'China'), ('Guangzhou', 'China'), ('Shenzhen', 'China'), ('Chengdu', 'China')
    ]
    city, country = random.choice(locations)
    return f'{city}, {country}'

# Generate random date within a range
def generate_random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

# Generate random price in USD
def generate_random_price(min_price, max_price):
    return round(random.uniform(min_price, max_price), 2)

# Generate records
def generate_records(num_records=1):
    records = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    min_price = 50
    max_price = 300
    
    for _ in range(num_records):
        booking_id = generate_random_id(5)
        user_id = generate_random_id(4)
        property_id = generate_random_id(3)
        location = generate_random_location()
        start_date = generate_random_date(start_date, end_date)
        end_date = generate_random_date(start_date, end_date)
        price = generate_random_price(min_price, max_price)
        
        record = {
            'bookingId': booking_id,
            'userID': user_id,
            'propertyId': property_id,
            'location': location,
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'price': price
        }
        records.append(record)
    
    return records

sqs = boto3.client('sqs')
sqs_URL = "https://sqs.ap-south-1.amazonaws.com/254241970276/AirbnbBookingQueue"

def lambda_handler(event, context):
    try:
        i = 0
        while(i < 5):
            data = generate_records()
            print(data)
            sqs.send_message(
                QueueUrl=sqs_URL,
                MessageBody=json.dumps(data)
            )
            i += 1
        print('Data published to SQS!')
        return {
            'statusCode': 200,
            'body': json.dumps('Data publish to SQS - SUCCESSFUL!')
        }
    except Exception as err:
        print(err)
        print('Data publish to SQS - FAILED!')
