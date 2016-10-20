# Real-Time-Data-Processing-using-Kafka-Spark-Streaming-and-Cassandra
This project aims to mine the logs with error status codes from Kafka real-time streaming data and save them into Cassandra database.

1. Set up Kafka: 
1.1. Install Kafka in Windows by following the instruction in the link below:
https://dzone.com/articles/running-apache-kafka-on-windows-os
1.2. Start Zookeeper server
 # cd C:\zookeeper\zookeeper-3.3.6\bin
 # zkserver
1.3. Start Kafka broker:
 # cd C:\kafka\kafka_2.11-0.10.0.1
 # .\bin\windows\kafka-server-start.bat .\config\server.properties
1.4. Create a topic "testLogs"
 # cd C:\kafka\kafka_2.11-0.10.0.1\bin\windows
 # kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testLogs
1.5. run the spark program (must run before the producer)
1.6. create a producer to send the web log to Kafka broker
 # cd C:\kafka\kafka_2.11-0.10.0.1\bin\windows
 # kafka-console-producer.bat --broker-list localhost:9092 --topic testLogs < web_log_data.txt

