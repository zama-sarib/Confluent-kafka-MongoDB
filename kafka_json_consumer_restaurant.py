import argparse
import pandas as pd
import json
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from database.mongo import MongodbOperation 


FILE_PATH = r"C:\Users\samri\Desktop\Sarib Workspace\Confluent-Kafka-Setup\restaurant_orders.csv" #"cardekho_dataset.csv"
columns=['Order Number', 'Order Date', 'Item Name', 'Quantity', 'Product Price',
       'Total products']

API_KEY = 'NSV6GA2BBBUMQPRE'
ENDPOINT_SCHEMA_URL  = 'https://psrc-4nyjd.us-central1.gcp.confluent.cloud'
API_SECRET_KEY = '1s9Eo/nrQzX5pDyfNuzVi8EBHvpF9lx7yAkZAt6qkFiA9JmcyZt+hSoqHu3zHpt1'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'GMQDNIMUUMDBTLGW'
SCHEMA_REGISTRY_API_SECRET = 'zrARpcI3s+rv6D7/JWGH1Y5qs3YP45QByGuzIZmJLl+qzhbWzB/qlhWvx+BVDNVR'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_Restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"
    
def car_to_dict(restaurant:Restaurant, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return restaurant.record

def get_schema_to_produce_consume_data(file_path=FILE_PATH):

    cols = next(pd.read_csv(file_path,chunksize=10)).columns
    schema = dict()
    schema.update({
    "$id": "http://example.com/myURI.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "additionalProperties": False,
    "description": "Sample schema to help you get started.",
    "properties": dict(),
    "title": "SampleRecord",
    "type": "object"})
    for column in cols:
        schema['properties'].update({
            f"{column}":{
                "description": f"restaurant {column}",
                "type":"string",
            }
    })
    schema = json.dumps(schema)
    return schema

def main(topic):

    schema = get_schema_to_produce_consume_data(FILE_PATH)
    json_deserializer = JSONDeserializer(schema,
                                         from_dict=Restaurant.dict_to_Restaurant)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    mongodb = MongodbOperation()
    records = []
    x = 0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if car is not None:
                records.append(car_to_dict(car,None))
                if x%500 == 0:
                # print("User record {}: car: {}\n"
                #       .format(msg.key(), car))
                    mongodb.insert_many(collection_name='restaurant',records=records)
                    records = []
                print("Inserted 500 records to database")
            x += 1

        except KeyboardInterrupt:
            break

    consumer.close()

main("Restaurant")