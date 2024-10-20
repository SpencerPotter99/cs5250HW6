import unittest
import boto3
import json
import time
from homework6 import WidgetConsumer 

BUCKET2_NAME = 'usu-cs5250-student-requests'
BUCKET_NAME = 'usu-cs5250-student-web'
DYNAMODB_TABLE = 'widgets'

class TestWidgetConsumerWithAWS(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.consumer = WidgetConsumer(storage_strategy='dynamodb')
        cls.dynamodb = boto3.resource('dynamodb').Table(DYNAMODB_TABLE)
        cls.s3 = boto3.client('s3')
        cls.s3.head_bucket(Bucket=BUCKET_NAME)


    def test_store_in_dynamodb(self):
        widget_data = {
            'widgetId': 'widget-1',
            'owner': 'Test Owner',
            'label': 'Test Widget',
            'description': 'This is a test widget.',
            'color': 'red',
            'height': '10',
            'length': '5',
            'note': 'Test note',
            'price': '20.0',
            'quantity': '100',
            'rating': '4.5',
            'size': 'large',
            'vendor': 'Test Vendor',
            'width': '15',
            'last_modified_on': '2024-09-22T22:00:01Z'
        }
        self.consumer.store_in_dynamodb(widget_data)

        # Verify that the item is in DynamoDB
        response = self.dynamodb.get_item(Key={'id': 'widget-1'})
        self.assertIn('Item', response)

    def test_process_create_request(self):
        widget_data = {
            'type': 'create',
            'requestId': 'test-request-2',
            'widgetId': 'widget-2',
            'owner': 'Test Owner',
            'label': 'Test Widget 2',
            'description': 'This is another test widget.',
            'color': 'blue',
            'height': '20',
            'length': '10',
            'note': 'Another test note',
            'price': '30.0',
            'quantity': '50',
            'rating': '4.0',
            'size': 'medium',
            'vendor': 'Another Vendor',
            'width': '25',
            'last_modified_on': '2024-09-22T22:00:01Z'
        }
        self.consumer.process_create_request(widget_data)
        
        response = self.dynamodb.get_item(Key={'id': 'widget-2'})
        self.assertIn('Item', response)

    def test_process_delete_request(self):
        widget_data = {
            'type': 'create',
            'requestId': 'test-request-3',
            'widgetId': 'widget-3',
            'owner': 'Test Owner',
            'label': 'Test Widget 3',
            'description': 'This is yet another test widget.',
            'color': 'green',
            'height': '15',
            'length': '7',
            'note': 'Delete test note',
            'price': '25.0',
            'quantity': '75',
            'rating': '3.5',
            'size': 'small',
            'vendor': 'Vendor Test',
            'width': '12',
            'last_modified_on': '2024-09-22T22:00:01Z'
        }
        self.consumer.process_create_request(widget_data)

        self.consumer.process_delete_request(widget_data)

        response = self.dynamodb.get_item(Key={'id': 'widget-3'})
        self.assertNotIn('Item', response)

    def test_get_widget_request(self):
        test_widget_request = {
            "type": "create",
            "requestId": "e80fab52-71a5-4a76-8c4d-11b66b83ca2a",
            "widgetId": "8123f304-f23f-440b-a6d3-80e979fa4cd6",
            "owner": "Mary Matthews",
            "label": "JWJYY",
            "description": "THBRNVNQPYAWNHGRGUKIOWCKXIVNDLWOIQTADHVEVMUAJWDONEPUEAXDITDSHJTDLCMHHSESFXSDZJCBLGIKKPUYAWKQAQI",
            "otherAttributes": [
                {"name": "width-unit", "value": "cm"},
                {"name": "length-unit", "value": "cm"},
                {"name": "rating", "value": "2.580677"},
                {"name": "note", "value": "FEGYXHIJCTYNUMNMGZBEIDLKXYFNHFLVDYZRNWUDQAKQSVFLPRJTTXARVEIFDOLTUSWZZWVERNWPPOEYSUFAKKAPAGUALGXNDOVPNKQQKYWWOUHGOJWKAJGUXXBXLWAKJCIVPJYRMRWMHRUVBGVILZRMESQQJRBLXISNFCXGGUFZCLYAVLRFMJFLTBOTLKQRLWXALLBINWALJEMUVPNJWWRWLTRIBIDEARTCSLZEDLZRCJGSMKUOZQUWDGLIVILTCXLFIJIULXIFGRCANQPITKQYAKTPBUJAMGYLSXMLVIOROSBSXTTRULFYPDFJSFOMCUGDOZCKEUIUMKMMIRKUEOMVLYJNJQSMVNRTNGH"}
            ]
        }
        
        self.s3.put_object(
            Bucket=BUCKET2_NAME,
            Key='test_widget_request.json',
            Body=json.dumps(test_widget_request),
            ContentType='application/json'
        )

        # Add a short delay to allow S3 to propagate the new object
        time.sleep(1)

        widget_request = self.consumer.get_widget_request()

        self.assertEqual(widget_request, test_widget_request)


        
if __name__ == '__main__':
    unittest.main()