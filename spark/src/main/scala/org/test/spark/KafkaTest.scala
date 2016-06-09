package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import com.datastax.spark.connector._
import scala.util.Try
import org.joda.time.DateTime

/** listening for log data from Kafka's test topic on default port 9092. */
object KafkaExample {
  
  def main(args: Array[String]) {

    val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .setMaster("local[*]")
        .setAppName("KafkaCassandraTest")
        
    // context with a 1 second batch interval
    val ssc = new StreamingContext(conf, Seconds(1))
    
    setupLogging()
    
    // regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka brokers
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topics = List("test").toSet
    // Create Kafka stream, which will contain (topic,message) pairs. We tack a 
    // map(_._2) at the end in order to only get the messages, individual lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
         
    // Extract the status field from each log line
    val statuses = lines.map(x => {
          val matcher:Matcher = pattern.matcher(x); 
          if (matcher.matches()) matcher.group(6) else "[error]"
        }
    )
    
    // Map these status results to success and failure
    val successFailure = statuses.map(x => {
      val statusCode = Try(x.toInt) getOrElse 0
      if (statusCode >= 200 && statusCode < 300) {
        "Success"
      } else if (statusCode >= 500 && statusCode < 600) {
        "Failure"
      } else {
        "Other"
      }
    })
    
    // Wrap up statuses over a 5 sec window sliding every 1 second
    val statusCounts = successFailure.countByValueAndWindow(Seconds(5), Seconds(1))
    
    var succErrNum = 0;
    var succVcNum = 0;
    
    // For each batch, get the RDD's representing data from our current window
    statusCounts.foreachRDD((rdd, time) => {
      // Keep track of total success and error codes from each RDD
      var totalSuccess:Long = 0
      var totalError:Long = 0
      var totalOther:Long = 0

      if (rdd.count() > 0) {
        succVcNum = 0
        val elements = rdd.collect()
        
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {
            totalSuccess += count
          }
          else if (result == "Failure") {
            totalError += count
          }
          else {
            totalOther += count
          }
        }
        
      } else {
        succVcNum += 1
        println("no data")
        if(succVcNum > 6)
          println("no data alarm")
      }

      println("Total success: " + totalSuccess + " Total failure: " + totalError)
      rdd.map( x => ((new DateTime(time.milliseconds)).toString(), totalSuccess, totalError, totalOther)).saveToCassandra("mykeyspace1", "logstatus", SomeColumns("datetime", "success", "failure", "other"))
      
      // Don't alarm unless we have some minimum amount of data to work with
      if (totalError + totalSuccess > 100) {
        // Compute the error rate
        // use of util.Try to handle potential divide by zero exception
        val ratio:Double = Try( totalError.toDouble / totalSuccess.toDouble ) getOrElse 1.0
        // If there are more errors than successes, wake someone up
        if (ratio > 0.5) {
          succErrNum += 1
          if(succErrNum >4) 
            println("Wake somebody up! Something is horribly wrong.")  //we can send email here, but lower than a certain frequency, like 30 mins a mail 
          
        } else {
          succErrNum = 0
          println("All systems go.")
        }
      }
    })

    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

