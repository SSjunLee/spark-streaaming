package com.ljn.ss

import com.ljn.ss.util.DbUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "vm:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pk-spark",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val type2dur = lines.map(
      x => {
        val arr = x.value().split(",")
        (arr(arr.length-2), arr(arr.length-1))
      }
    )
    type2dur.map(x=>(x._1,1)).reduceByKey(_+_).foreachRDD(x=>{
      x.foreachPartition(
        par=>{
          val col = DbUtils.getCol()
          par.foreach(record=>{
              DbUtils.inc(record._1,record._2.toInt,col)
          })
        }
      )
    })
    ssc.start()
    ssc.awaitTermination()
    DbUtils.close()
  }
}
