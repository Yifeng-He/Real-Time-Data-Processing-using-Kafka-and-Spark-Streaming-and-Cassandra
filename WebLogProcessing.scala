
package com.cloudmedia

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import org.apache.kafka.clients.consumer.ConsumerRecord //kafka_2.10-0.10.0.1
import org.apache.kafka.common.serialization.StringDeserializer //kafka_2.10-0.10.0.1
import org.apache.spark.streaming.kafka010._ //spark-streaming-kafka-0-10_2.11-2.0.1
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent //spark-streaming-kafka-0-10_2.11-2.0.1
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe //spark-streaming-kafka-0-10_2.11-2.0.1

import scala.util.matching.Regex


/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object WebLogProcessing {

  // function to extract time from the web log
  def extractTime(str_input : String) : String = {
    val pattern = """\[.*\]""".r
    val str_time = pattern.findFirstMatchIn(str_input).getOrElse("NA").toString() 
    if (str_time != "NA") {
      str_time.split(" ")(0).stripPrefix("[")
    }
    else {
      str_time
    }
  } 
 
  // function to extract IP address from the web log
  def extractIP (str_input : String) : String = {
    val pattern = """\d{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}""".r
    pattern.findFirstMatchIn(str_input).getOrElse("NA").toString()
  }

  // function to extract the requested URL
  def extractURL(str_input : String) : String = {
    val pattern = new Regex("(GET|POST).*HTTP")
    val str_URL = pattern.findFirstMatchIn(str_input).getOrElse("NA").toString()
    if(str_URL !="NA"){
      str_URL.toString().split(" ")(1)
    }
    else {
      str_URL
    }      
  }    
  
  // function to extract the status code from the web log
  def extractStatusCode(str_input : String) : Int = {
    val pattern = new Regex("\" [0-9][0-9][0-9]") 
    val matched_str = pattern.findFirstMatchIn(str_input).getOrElse("0").toString()
    if (matched_str != "0") {
      matched_str.toString().split(" ")(1).toInt
    }
    else {
      matched_str.toInt
    }
  }

  // function to extract user agent from the web log
  def extractAgent(str_input : String) : String = {
    val pattern = new Regex("\" \".+\"$")
    val str_agent = pattern.findFirstMatchIn(str_input).getOrElse("NA").toString()
    if(str_agent != "NA"){
      str_agent.stripPrefix("\"").stripSuffix("\"").trim.stripPrefix("\"")
    }
    else {
      str_agent
    }      
  } 

  // set the log level to ERROR 
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }  
  
  def main(args: Array[String]) {
 
    // Set up the spark configuration
    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    conf.setMaster("local[*]")
    conf.setAppName("WebLogProcessing")
    
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(1))
    
    // set up log level
    setupLogging()

    //the parameters for connecting to Kafka broker
    val kafkaParams = Map[String, Object]( 
        "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "example",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean) )
    
    // the Kafka topic to be subscribed
    val topics = Array("testLogs")
    
    // get input stream from Kafka broker 
    val input_stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,Subscribe[String, String](topics, kafkaParams))
    
    // the DStream containing the log lines
    val dstream_lines = input_stream.map(record => record.value) 
    // get the DStream containing tuples of (Time, IP, URL, Status, UserAgent)
    val dstream_tuples = dstream_lines.map(line => (extractTime(line), extractIP(line), extractURL(line), extractStatusCode(line), extractAgent(line)))
    
    // process the log: to save the logs with status of error into Cassandra database
    dstream_tuples.foreachRDD((rdd, time) => {
      // filter the RDD
      val filtered_RDD = rdd.filter(tuple => tuple._4 > 499)
      if (filtered_RDD.count() > 0) {
        println("Writing " + filtered_RDD.count() + " rows to Cassandra")
        // save the log with error status to Cassandra database
        rdd.saveToCassandra("WebLogDatabase", "WebLog", SomeColumns("Time", "IP", "URL", "Status", "UserAgent"))
      }
    })
  
    // start spark streaming program
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
