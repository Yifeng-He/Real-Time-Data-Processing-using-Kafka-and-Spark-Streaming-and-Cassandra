# Real-Time-Data-Processing-using-Kafka-Spark-Streaming-and-Cassandra
This project aims to mine the logs with error status codes from Kafka real-time streaming data and save them into Cassandra database.

* Step 1. Set up Cassandra

1.1 Download DataStax Distribution of Apache Cassandra from the link below, and then install it in Windows

http://www.planetcassandra.org/cassandra/

1.2. Start Cassandra server:

  # cd C:\cassandra\apache-cassandra\bin

  # cassandra

1.3. Open Datastax DevCenter, and execute the folowing cql in orders:

  # CREATE KEYSPACE WebLogDatabase WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};

  # USE WebLogDatabase; CREATE TABLE WebLog (Time text PRIMARY KEY, IP text, URL text, Status varint, UserAgent text);

  # USE WebLogDatabase; INSERT INTO WebLog (Time, IP, URL, Status, UserAgent) VALUES ('29/Nov/2015:03:50:05', '156.23.45.89', '/news', 520, 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0)');

  # USE WebLogDatabase; SELECT * FROM WebLog;

* Step 2. Set up Kafka: 

2.1. Install Kafka in Windows by following the instruction in the link below:

https://dzone.com/articles/running-apache-kafka-on-windows-os

2.2. Start Zookeeper server

  # cd C:\zookeeper\zookeeper-3.3.6\bin
 
  # zkserver
 
2.3. Start Kafka broker:

 # cd C:\kafka\kafka_2.11-0.10.0.1
 
 # .\bin\windows\kafka-server-start.bat .\config\server.properties
 
2.4. Create a topic "testLogs"

 # cd C:\kafka\kafka_2.11-0.10.0.1\bin\windows
 
 # kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testLogs
 
* Step 3. Run the spark program 

* Step 4. Create a producer to send the web log to Kafka broker

  # cd C:\kafka\kafka_2.11-0.10.0.1\bin\windows
 
  # kafka-console-producer.bat --broker-list localhost:9092 --topic testLogs < web_log_data.txt
 

