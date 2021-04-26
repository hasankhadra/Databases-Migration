import boto3
from pg_db import PG_SQL
from boto3.dynamodb.conditions import Key, Attr

ipv4 = 'localhost'

from datetime import datetime
class DynamoDB:

    def resource(self):
        conn = boto3.resource('dynamodb', endpoint_url=f"http://{ipv4}:8000")
        return conn

    def client(self):
        conn = boto3.client('dynamodb', endpoint_url=f"http://{ipv4}:8000")
        return conn

    def create_table(self, name, attributes, types):
        conn = self.resource()
        print(name)
        if name == "rental_payment":
            key_schema = [
                {
                    'AttributeName': attributes[0],
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': attributes[1],
                    'KeyType': 'RANGE'  # Partition key
                }
            ]
            # print(key_schema)
            attributes_definitions = [{'AttributeName': attributes[0],
                                       'AttributeType': self.get_type(types[0])},
                                      {'AttributeName': attributes[1],
                                       'AttributeType': self.get_type(types[1])}
                                      ]
        else:
            key_schema = [
                {
                    'AttributeName': attributes[0],
                    'KeyType': 'HASH'  # Partition key
                }
            ]
            # print(key_schema)
            attributes_definitions = [{'AttributeName': attributes[0],
                                       'AttributeType': self.get_type(types[0])}
                                      ]
        table = conn.create_table(
            TableName=name,
            KeySchema=key_schema,
            AttributeDefinitions=attributes_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        return table

    def delete_table(self, name):
        conn = self.client()

        response = conn.delete_table(TableName=name)
        return response

    def get_type(self, type):
        if type == 'integer':
            return 'N'
        if type == 'bytea' or type == 'B':
            return 'B'
        return 'S'

    def insert_item(self, name, types, attributes, row):
        conn = self.client()

        item = {
            attributes[x]: {
                self.get_type(types[x]): str(row[x])
            }
            for x in range(len(attributes))
        }

        response = conn.put_item(
            TableName=name,
            Item=item,
            ReturnConsumedCapacity='TOTAL'
        )
        return response

    def delete_item(self, name, id):
        conn = self.client()

        item = {
            f'{name}_id': {'N': str(id)}
        }
        response = conn.delete_item(
            TableName=name,
            Key=item
        )

    def insert_items(self, name, types, attributes, rows):
        conn = self.client()
        requestitems = {name: [{
            'PutRequest':{
                "Item":{
            attributes[x]: {
                self.get_type(types[x]): str(row[x])
            }
            for x in range(len(attributes))
        }}} for row in rows]}

        response = conn.batch_write_item(
            RequestItems=requestitems
        )



    def get_item(self, name, Id):
        conn = self.client()

        response = conn.get_item(
            TableName=name,
            Key={f'{name}_id': {'N': str(Id)}}
        )

        return response

    def get_items(self, name):
        conn = self.resource()

        table = conn.Table(name)
        response = table.scan()
        data = response["Items"]
        return data

    def update_item(self, name, types, attribute, values):
        conn = self.client()

        response = conn.update_item(
            ExpressionAttributeNames={
                '#AT': attribute[1],
                '#Y': attribute[2],
            },
            ExpressionAttributeValues={
                ':t': {
                    self.get_type(types[1]): values[1],
                },
                ':y': {
                    self.get_type(types[2]): values[2],
                },
            },
            Key={
                f'{name}_id': {
                    'N': str(values[0]),
                }
            },
            ReturnValues='ALL_NEW',
            TableName=name,
            UpdateExpression='SET #Y = :y, #AT = :t',
        )
        return response


    def describe_table(self, name):
        conn = self.client()

        response = conn.describe_table(TableName=name)
        return response

    def list_tables(self):
        conn = self.client()
        response = conn.list_tables(
            Limit=99
        )
        return response['TableNames']


    def reset(self):
        tables = self.list_tables()
        for table in tables:
            self.delete_table(table)

    def query(self, rental_id, payment_id):
        conn = self.resource()
        table = conn.Table('rental_payment')
        amount = table.query(
            KeyConditionExpression=Key('rental_id').eq(rental_id) & Key('payment_id').eq(payment_id)
        )['Items'][0]['amount']
        response = table.scan(
            Select='COUNT',
            FilterExpression=Key('amount').lt(amount)
        )
        return response

    def given_query(self, limit=10):

        rows = self.get_items('rental_payment')
        if len(rows) > limit:
            rows = rows[:limit]
        print("rental_id  ", "payment_id  ", "COUNT")
        results = []
        for row in rows:
            result = self.query(row['rental_id'], row['payment_id'])
            results.append(result)
            print(row['rental_id'], "      ", row['payment_id'], "      ",  result['Count'])

        return results


if __name__ == '__main__':
    Dynamo_inst = DynamoDB()
    PG_inst = PG_SQL()
    Dynamo_inst.reset()
    """
    don't delete this comment, it's for the presentation
    
    print(Dynamo_inst.list_tables())
    items = Dynamo_inst.get_items('rental')
    for item in items:
        print(item)
        
    Dynamo_inst.given_query()
    """
