import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, PRODUCE_TOPIC_RIDES_CSV,FHVH_RIDES_DATA_PATH,GREEN_RIDES_DATA_PATH, \
PRODUCE_TOPIC_FHVH_RIDES_CSV,PRODUCE_TOPIC_GREEN_RIDES_CSV


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[9]}, {row[16]}')
                ride_keys.append(str(row[0]))
                i += 1
                if i == 0:
                    break
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


class GreenRidesCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count
                records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}')
                ride_keys.append(str(row[0]))
                i += 1
                if i == 25:
                    break
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)

class FhvRidesCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count
                records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}')
                ride_keys.append(str(row[0]))
                i += 1
                if i == 25:
                    break
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)        

if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    
    producer = RideCSVProducer(props=config)
    ride_records = producer.read_records(resource_path=INPUT_DATA_PATH)
    print(ride_records)
    producer.publish(topic=PRODUCE_TOPIC_RIDES_CSV, records=ride_records)

    producer = GreenRidesCSVProducer(props=config)
    ride_records = producer.read_records(resource_path=GREEN_RIDES_DATA_PATH)
    print(ride_records)
    producer.publish(topic=PRODUCE_TOPIC_GREEN_RIDES_CSV, records=ride_records)

    producer = FhvRidesCSVProducer(props=config)
    ride_records = producer.read_records(resource_path=FHVH_RIDES_DATA_PATH)
    print(ride_records)
    producer.publish(topic=PRODUCE_TOPIC_FHVH_RIDES_CSV, records=ride_records)
