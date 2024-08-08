from confluent_kafka import Consumer, KafkaError, KafkaException
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import json


class KafkaConsumer:
    def __init__(self, topic):
        conf = {'bootstrap.servers': 'localhost:9092',
                'group.id': 'visualization',
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest'}

        self.consumer = Consumer(conf)
        self.consumer.subscribe(topic)

        self.running = True

        self.time = []
        self.open = []
        self.high = []
        self.low = []
        self.close = []

        self.fig = mpf.figure(style='charles', figsize=(8, 8))
        self.ax = self.fig.add_subplot(1, 1, 1)
        plt.ion()

    def read(self):
        print('<===================== Reading Messages =====================>')
        while self.running:
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f'End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
                    else:
                        raise KafkaException(msg.error())
                else:
                    value = msg.value().decode('utf-8')
                    self.appendData(json.loads(value))
                    self.updatePlot()
                    print(json.loads(value))

            except KeyboardInterrupt:
                break

        self.consumer.close()

    def appendData(self, data):
        # Append new data to the lists
        self.time.append(data['start'][:-10])
        self.open.append(data['open'])
        self.high.append(data['high'])
        self.low.append(data['low'])
        self.close.append(data['close'])

    def updatePlot(self):
        df = pd.DataFrame({
            'Date': self.time,
            'Open': self.open,
            'High': self.high,
            'Low': self.low,
            'Close': self.close
        })

        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        # Plot candlestick chart
        self.ax.clear()
        mpf.plot(df, type='candle', ax=self.ax, ylabel='Price')
        plt.draw()
        plt.pause(0.3)


if __name__ == '__main__':
    cons = KafkaConsumer(['stock-ohlc'])
    cons.read()
