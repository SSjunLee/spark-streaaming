package com.ljn

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.MongoUtil

import scala.collection.mutable.ArrayBuffer

object Main {

  val lst: ArrayBuffer[(Int, Int)] = ArrayBuffer()
  lst += Tuple2(0, 10 * 60)
  lst += Tuple2(10 * 60, 20 * 60)
  lst += Tuple2(20 * 60, Integer.MAX_VALUE)

  def getType(value: Int): Int = {
    for (i <- 0 until lst.length) {
      if (value > lst(i)._1 && value < lst(i)._2)
        return i
    }
    return lst.length
  }

  def main(args: Array[String]): Unit = {

    var sparkConf = new SparkConf().setAppName("NetworkWordCount")
    if(args.length == 0)
      sparkConf = sparkConf.setMaster("local[2]")
    val Array(zkQuorum, group, topics, numThreads) = Array[String]("vm:2181", "test", "t1", "1")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)


    val msgStream = messages.map(
      item => {
        val arr = item._2.split(",")
        val size = arr.size
        Msg(arr(size - 2), arr(size - 1).toInt)
      }
    )
    msgStream.map(item => (getType(item.dur), 1)).reduceByKey(_ + _)
      .foreachRDD(
        rdd => {
          rdd.foreachPartition(partition => {
            partition.foreach(record => {
              println(record)
              MongoUtil.inc(record._1, record._2, "tb_dur", "dur_type")
            })
          })
        }
      )

    msgStream.transform { rdd => {
      rdd.map(item => (item.videoType, 1))
    }
    }.reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partiton => {
        partiton.foreach(record => {
          println(record)
          MongoUtil.inc(record._1, record._2, "tb_type", "video_type")
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  case class Msg(videoType: String, dur: Int);

}
