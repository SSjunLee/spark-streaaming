package com.ljn.ss
import com.ljn.ss.manager.{MongodbOffserManager, OffsetManager}
import com.ljn.ss.util.DbUtils
import com.mongodb.client.model.Filters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

object OffsetApp {



  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(8))
    val offsetManager:OffsetManager = MongodbOffserManager
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "vm:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pk-spark",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val partitionToLong = offsetManager.
        obtainOffsets("test",0,new DbUtils.DbConfig("vm", 27017, "ljn"))


    val topics = Array("test")
    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams,partitionToLong)
    )

    lines.foreachRDD(rdd=>{
      val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if(!rdd.isEmpty())
        println("-------------------------------"+rdd.count()+"------------------------------------------")
      ranges.map(x=>{
        offsetManager.storeOffsets(x.topic,x.partition,x.untilOffset,new DbUtils.DbConfig("vm", 27017, "ljn"))
        x
      })
      rdd
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
