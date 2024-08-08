from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, struct, col, window, first, last, max, min
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType


class SparkApp:
    def __init__(self):
        self.spark = (SparkSession
                      .builder
                      .appName('Stock Streaming')
                      .master('local[1]')
                      .config('spark.sql.shuffle.partitions', 1)
                      .config('spark.executor.memory', '2g')
                      .config("spark.streaming.stopGracefullyOnShutdown", "true")
                      .config('spark.jars',
                              'dependencies/spark-sql-kafka-0-10_2.12-3.3.0.jar,'
                              'dependencies/kafka-clients-3.3.0.jar,'
                              'dependencies/spark-token-provider-kafka-0-10_2.12-3.3.0.jar,'
                              'dependencies/commons-pool2-2.12.0.jar')
                      .getOrCreate())

        self.schema = StructType([
            StructField('timestamp', TimestampType(), False),
            StructField('price', FloatType(), False)
        ])
        self.record = None
        self.streamingQuery = None

    def readStream(self):
        try:
            rawDF = (self.spark
                     .readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", "localhost:9092")
                     .option("subscribe", "stock")
                     .option("startingOffsets", "earliest")
                     .option("failOnDataLoss", "false")
                     .load()
                     .select(from_json(col('value').cast('string'), self.schema).alias('value'))
                     .select('value.*')
                     )

            self.record = (rawDF
                           .withWatermark('timestamp', '1 minute')
                           .groupby(window(col('timestamp'), '1 minute').alias('time'))
                           .agg(first('price').alias('open'),
                                max('price').alias('high'),
                                min('price').alias('low'),
                                last('price').alias('close'))
                           .select('time.*', 'open', 'high', 'low', 'close')
                           )

            self.streamingQuery = (self.record
                                   .select(col('start').cast('string').alias('key'),
                                           to_json(struct('*')).alias('value'))
                                   .writeStream
                                   .format('kafka')
                                   .option("kafka.bootstrap.servers", "localhost:9092")
                                   .option("topic", "stock-ohlc")
                                   .outputMode('append')
                                   .trigger(processingTime='5 second')
                                   .option('checkpointLocation', 'chkpntDir')
                                   .start()
                                   )

            self.streamingQuery.awaitTermination()

        finally:
            self.spark.stop()


if __name__ == '__main__':
    app = SparkApp()
    app.readStream()
