import argparse
import boto3
import json
import time
import jsonschema
from jsonschema import validate
import logging

logging.basicConfig(
    filename='consumer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BUCKET2 = 'usu-cs5250-student-requests'
BUCKET3 = 'usu-cs5250-student-web'
DYNAMODB_TABLE = 'widgets'

class WidgetConsumer:
    def __init__(self, storage_strategy):
        self.storage_strategy = storage_strategy
        self.s3 = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb').Table(DYNAMODB_TABLE)

    @staticmethod
    def validate_widget_request(widget_request):
        widget_request_schema = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "pattern": "create|delete|update"
                },
                "requestId": {
                    "type": "string"
                },
                "widgetId": {
                    "type": "string"
                },
                "owner": {
                    "type": "string",
                    "pattern": "[A-Za-z ]+"
                },
                "label": {
                    "type": "string"
                },
                "description": {
                    "type": "string"
                },
                "otherAttributes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string"
                            },
                            "value": {
                                "type": "string"
                            }
                        },
                        "required": ["name", "value"]
                    }
                }
            },
            "required": ["type", "requestId", "widgetId", "owner"]
        }

        try:
            validate(instance=widget_request, schema=widget_request_schema)
            return True
        except jsonschema.exceptions.ValidationError as err:
            logging.error(f"Invalid Widget Request: {err.message}")
            return False

    def get_widget_request(self):
        response = self.s3.list_objects_v2(Bucket=BUCKET2, MaxKeys=1)
        if 'Contents' in response and len(response['Contents']) > 0:
            key = response['Contents'][0]['Key']
            widget_request = self.s3.get_object(Bucket=BUCKET2, Key=key)
            widget_data = json.loads(widget_request['Body'].read().decode('utf-8'))

            self.s3.delete_object(Bucket=BUCKET2, Key=key)
            return widget_data
        return None

    def process_create_request(self, widget):
        if self.storage_strategy == 'bucket3':
            self.store_in_bucket3(widget)
        elif self.storage_strategy == 'dynamodb':
            self.store_in_dynamodb(widget)

    def store_in_bucket3(self, widget):
        key = f"widgets/{widget['owner'].replace(' ', '-').lower()}/{widget['widgetId']}"
        self.s3.put_object(
            Bucket=BUCKET3,
            Key=key,
            Body=json.dumps(widget),
            ContentType='application/json'
        )
        logging.info(f"Widget stored in Bucket 3: {key}")

    def store_in_dynamodb(self, widget):
        item = {
            'widget_id': widget['widgetId'],
            'owner': widget['owner'],
            'label': widget['label'],
            'description': widget['description'],
        }

        for attr in widget.get('otherAttributes', []):
            item[attr['name']] = attr['value']
        
        self.dynamodb.put_item(Item=item)
        logging.info(f"Widget stored in DynamoDB: {widget['widgetId']}")

    def process_delete_request(self, widget):
        widget_id = widget['widgetId']
        if self.storage_strategy == 'bucket3':
            key = f"widgets/{widget['owner'].replace(' ', '-').lower()}/{widget_id}"
            try:
                self.s3.delete_object(Bucket=BUCKET3, Key=key)
                logging.info(f"Widget deleted from Bucket 3: {key}")
            except Exception as e:
                logging.warning(f"Failed to delete widget from Bucket 3: {e}")

        elif self.storage_strategy == 'dynamodb':
            try:
                self.dynamodb.delete_item(Key={'widget_id': widget_id})
                logging.info(f"Widget deleted from DynamoDB: {widget_id}")
            except Exception as e:
                logging.warning(f"Failed to delete widget from DynamoDB: {e}")

    def poll_requests(self):
        while True:
            widget_request = self.get_widget_request()
            if widget_request:
                if self.validate_widget_request(widget_request):
                    if widget_request['type'] == 'create':
                        self.process_create_request(widget_request)
                    elif widget_request['type'] == 'delete':
                        self.process_delete_request(widget_request)
                else:
                    logging.warning("Skipping invalid widget request.")
            else:
                time.sleep(0.1)

def main():
    parser = argparse.ArgumentParser(description='Process widget requests.')
    parser.add_argument('--storage', '-s', required=True, choices=['bucket3', 'dynamodb'],
                        help='Storage strategy (bucket3 or dynamodb)')
    args = parser.parse_args()

    consumer = WidgetConsumer(storage_strategy=args.storage)
    consumer.poll_requests()

if __name__ == '__main__':
    main()
