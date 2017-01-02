package org.test.spark

/*why must put the kafka.serializer at the beginning???!!!*/
import kafka.serializer.StringDecoder
/*why must put the kafka.serializer at the beginning???!!!*/

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import org.apache.spark.streaming.kafka._

import com.datastax.spark.connector._
import scala.util.Try
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD

/** listening for log data from Kafka's test topic on default port 9092. */
object KafkaExample {

  // structured data for one line log
  case class LogEntry(ip:String, client:String, user:String, dateTime:String, request:String, status:String, bytes:String, referer:String, agent:String, count:Long)    

  // regular expression (regex) to extract fields from raw Apache log lines
  val pattern = apacheLogPattern()
    
  def parseLog(dIn:(String, Long)) : LogEntry = {     
     val x:String = dIn._1 
     val count:Long = dIn._2
     val matcher:Matcher = pattern.matcher(x);
     if (matcher.matches()) {
       return LogEntry(
           matcher.group(1),
           matcher.group(2),
           matcher.group(3),
           matcher.group(4),
           matcher.group(5),
           matcher.group(6),
           matcher.group(7),
           matcher.group(8),
           matcher.group(9),
           count
           )
     } else {
       return LogEntry("", "", "", "", "", "9999", "", "", "", 0)
     }
   } 
  
  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {
  
    @transient  private var instance: SparkSession = _
  
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }  
  
  
  def main(args: Array[String]) {

    val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .setMaster("local[*]")
        .setAppName("KafkaCassandraTest")
        
    // context with a 1 second batch interval
    val ssc = new StreamingContext(conf, Seconds(1))
    
    setupLogging()

    // hostname:port for Kafka brokers
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topics = List("test").toSet
    // Create Kafka stream, which will contain (topic,message) pairs.
    // map(_._2) at the end in order to only get the messages, individual lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
             

    var succVcNum = 0;
    var succErrNum = 0;
    //val statuses = lines.foreachRDD{ (rdd, time) =>
    //.foreachRDD{ (rdd: RDD[String], time: Time) =>    
    lines.countByValueAndWindow(Seconds(5), Seconds(1)).foreachRDD{ (rdd, time) =>
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val dsLog = rdd.map(parseLog).toDS().cache()
      
      val dsLog2 = dsLog.select(dsLog("count"),
        when(dsLog("status").between(500, 599), "Failure")
        .when(dsLog("status").between(200, 299), "Success")
        .otherwise("Other").as("status")
        ).groupBy("status").agg(sum('count) as "count").cache()
      
      if(dsLog2.count() > 0) {
        dsLog2.show()
        succVcNum = 0;

        def getStatusNum(sta: String):Long =  {
          val query = s"status = '$sta'"
          Try(dsLog2.select(dsLog2("count")).where(query).first().getAs[Long](0)) getOrElse 0
        }

        val totalSuccess:Long = getStatusNum("Success")
        val totalError:Long = getStatusNum("Failure")
        val totalOther:Long = getStatusNum("Other")
        
        val tupleTmp = (
            (new DateTime(time.milliseconds)).toString(), 
            totalSuccess, totalError, totalOther
        )
        
        spark.sparkContext.parallelize(Seq((tupleTmp))).
          saveToCassandra("mykeyspace1", "logstatus", SomeColumns("datetime", "success", "failure", "other"))          
       
        // Don't alarm unless we have some minimum amount of data to work with
        if(totalError + totalSuccess + totalOther > 100) {
          val ratio:Double = Try(( totalError.toDouble + totalOther) / totalSuccess.toDouble ) getOrElse 1.0
          // If there are too many errors , wake someone up
          if (ratio > 0.3) {
            succErrNum += 1
            if(succErrNum >4) 
              println("Wake somebody up! Something is horribly wrong.")  //we can send email here, but lower than a certain frequency, like 30 mins a mail 
          } else {
            succErrNum = 0
            println("All systems go.")
          }
        }
          
      } else {
        succVcNum += 1;
        println("no data")
        if(succVcNum > 10)
          println("no data alarm")        
      }
   }
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

