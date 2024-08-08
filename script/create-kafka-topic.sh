kafka-topics.sh --bootstrap-server localhost:9092 --topic stock --delete
kafka-topics.sh --bootstrap-server localhost:9092 --topic stock --create
kafka-topics.sh --bootstrap-server localhost:9092 --topic stock-ohlc --delete
kafka-topics.sh --bootstrap-server localhost:9092 --topic stock-ohlc --create
kafka-topics.sh --bootstrap-server localhost:9092 --topic __consumer_offsets --delete
