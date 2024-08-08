from confluent_kafka import Producer
import socket
import time
import json


class KafkaProducer:
    def __init__(self, topic):
        self.topic = topic
        conf = {'bootstrap.servers': 'localhost:9092',
                'client.id': socket.gethostname()}
        self.producer = Producer(conf)

    def send(self, key, message):
        self.producer.produce(self.topic, key=key, value=message)
        self.producer.flush()


if __name__ == '__main__':
    try:
        prd = KafkaProducer('stock')
        with open('data/trade_demo.csv') as file:
            counter = 0  # Keeping for Testing
            print('<===================== Sending Messages =====================>')
            for line in file:
                word = line.split(',')
                jsonString = json.dumps({
                    'timestamp': word[1],
                    'price': float(word[2])
                })
                # Exit after 100 messages
                if counter > 100:
                    break
                prd.send(word[0], jsonString)
                time.sleep(0.5)
    finally:
        print('<===================== Done =====================>')
