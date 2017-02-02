package org.test.spark

/*why must put the kafka.serializer at the beginning???!!!*/
//import kafka.serializer.StringDecoder
/*why must put the kafka.serializer at the beginning???!!!*/

import org.apache.spark.sql.functions.{when, window}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/*
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import com.datastax.spark.connector._
*/
import Utilities._

import java.util.regex.Pattern
import java.util.regex.Matcher
import java.util.Locale
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.Try

/** listening for log data from Kafka's test topic on default port 9092. */
object KafkaExample {

  val pattern = apacheLogPattern()
  val datePattern = Pattern.compile("\\[(.*?) .+]")    
 
  def parseDateField(field: String): Option[Timestamp] = {
      val dateMatcher = datePattern.matcher(field)
      if (dateMatcher.find) {
              val dateString = dateMatcher.group(1)
              val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
              val date = (dateFormat.parse(dateString))
              val timestamp = new Timestamp(date.getTime());
              //return Option(timestamp.toString())    //String can also be recognized as timestamp by sql.functions.window
              Option(timestamp)
          } else {
          None
      }
   } 
  
  case class LogEntry(ip:String, client:String, user:String, dateTime:Timestamp, request:String, status:Int, bytes:String, referer:String, agent:String)
  def parseLog(dIn:String) : Option[LogEntry] = {
     val x:String = dIn
     val matcher:Matcher = pattern.matcher(x);
     if (matcher.matches()) {
       Option(LogEntry(
           matcher.group(1),
           matcher.group(2),
           matcher.group(3),
           parseDateField(matcher.group(4)).getOrElse(null),
           matcher.group(5),
           Try(matcher.group(6).toInt) getOrElse(0),
           matcher.group(7),
           matcher.group(8),
           matcher.group(9)
           ))
     } else {
       None
     }
   } 
  
  def main(args: Array[String]) {
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()    

    setupLogging()      
      
    import spark.implicits._
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val dsRaw = lines.map(line => parseLog(line).getOrElse(null))
        .withWatermark("dateTime", "3 hours")
        .select(when($"status".between(500, 599), "Failure")
          .when($"status".between(200, 299), "Success")
          .otherwise("Other").as("status"), $"dateTime")
        .groupBy($"status", window($"dateTime", "2 hours", "2 hours").as("dt"))
        .count().orderBy("dt")

    import org.apache.spark.sql.streaming.ProcessingTime
    import scala.concurrent.duration._
    val query = dsRaw.writeStream          
      .trigger(ProcessingTime(1.seconds))
      .outputMode(OutputMode.Complete())  //.outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}

