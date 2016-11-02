
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.kafka.clients.consumer.ConsumerRecord 
import org.apache.kafka.common.serialization.StringDeserializer 
import org.apache.spark.streaming.kafka010._ 
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent 
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe 
import com.datastax.spark.connector._
import scala.util.matching.Regex


object WebLogProcessing {

  // extract (Time, IP, URL, Status, UserAgent) from a line of web log like this:
  // 66.249.75.168 - - [29/Nov/2015:06:24:05 +0000] "GET /national/?pg=1 HTTP/1.1" 200 11100 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
  def extractInformation (input_string : String) : (String, String, String, Int, String) = {
      // pattern for time
      val pattern_time = """\[.*\]""".r
      // pattern for IP address
      val pattern_IP = """\d{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}""".r
      // pattern for URL
      val pattern_URL = new Regex("(GET|POST).*HTTP")
      // pattern for status code
      val pattern_status = new Regex("\" [0-9][0-9][0-9]")
      // pattern for user agent
      val pattern_agent = new Regex("\" \".+\"$")
      
      // extract time
      val str_time_raw = pattern_time.findFirstMatchIn(input_string).getOrElse("NA").toString() 
      val str_time = {
        if (str_time_raw != "NA") {
          str_time_raw.split(" ")(0).stripPrefix("[")
        }
        else {
          str_time_raw
        }
      }
      
      // extract IP address
      val str_IP = pattern_IP.findFirstMatchIn(input_string).getOrElse("NA").toString()
      
      // extract URL
      val str_URL_raw = pattern_URL.findFirstMatchIn(input_string).getOrElse("NA").toString()
      val str_URL = {
        if(str_URL_raw !="NA"){
          str_URL_raw.toString().split(" ")(1)
        }
        else {
          str_URL_raw
        }
      }
      
      // extract status code
      val str_status_raw = pattern_status.findFirstMatchIn(input_string).getOrElse("0").toString()
      val status = {
        if (str_status_raw != "0") {
          str_status_raw.toString().split(" ")(1).toInt
        }
        else {
          str_status_raw.toInt
        }
      }
      
      // extract user agent
      val str_agent_raw = pattern_agent.findFirstMatchIn(input_string).getOrElse("NA").toString()
      val str_agent = {
        if(str_agent_raw != "NA"){
          str_agent_raw.stripPrefix("\"").stripSuffix("\"").trim.stripPrefix("\"")
        }
        else {
          str_agent_raw
        }
      }
      
      // return a tuple
      (str_time, str_IP, str_URL, status, str_agent)   
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
    val dstream_tuples = dstream_lines.map(extractInformation)
    
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
