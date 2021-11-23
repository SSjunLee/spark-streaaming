package com.ljn.ss

import com.ljn.ss.util.{ConfigUtil, DbUtils, RankUtils}
import com.mongodb.client.model.Filters
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document

object StreamApp {

  def initApp(cu: ConfigUtil) = {
    DbUtils.init(cu.get(ConfigUtil.MONGO_IP), cu.get(ConfigUtil.MONGO_PORT).toInt, cu.get(ConfigUtil.MONGO_DBNAME))
    RankUtils.initRankUtil(null)
  }

  def getKafkaStrategy(cu: ConfigUtil) = {
    val ip = cu.get(ConfigUtil.KAFKA_IP)
    val port = cu.get(ConfigUtil.KAFKA_PORT)
    val topic = cu.get(ConfigUtil.KAFKA_TOPIC)
    val serversStr = ip + ":" + port
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> serversStr,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pk-spark",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(topic)
    Subscribe[String, String](topics, kafkaParams)
  }

  def getStreamingContext(cu: ConfigUtil) = {
    val sparkConf = new SparkConf()
    if (cu.get(ConfigUtil.ENV).equals("test"))
      sparkConf.setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val second = cu.get(ConfigUtil.INTERVAL).toInt
    new StreamingContext(sparkConf, Seconds(second))
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("input config path")
      System.exit(-1)
    }
    val cu = new ConfigUtil(args(0))
    initApp(cu)


    val ssc = getStreamingContext(cu)
    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      getKafkaStrategy(cu)
    )
    val type2dur = lines.map(
      x => {
        val arr = x.value().split(",")
        (arr(arr.length - 2).trim, arr(arr.length - 1).trim)
      }
    ).filter(x => {
      val regex = """^\d+$""".r
      regex.findFirstMatchIn(x._2) != None
    })


    type2dur.map(x => (x._1, 1)).reduceByKey(_ + _).foreachRDD(x => {
      x.foreachPartition(
        par => {
          val col = DbUtils.getCol("wc")
          par.foreach(record => {
            val cond = Filters.eq("word", record._1)
            val kv = new Document("cnt", record._2)
            DbUtils.inc(cond, kv, col)
          })
        }
      )
    })
    type2dur.map(x => {
      val dur = x._2
      (RankUtils.getRank(dur.toInt), 1)
    }).reduceByKey(_ + _)
      .foreachRDD(x => x.foreachPartition(par => {
        val col = DbUtils.getCol("tb_rank")
        par.foreach(record => {
          val cond = Filters.eq("rank", record._1)
          val kv = new Document("cnt", record._2)
          DbUtils.inc(cond, kv, col)
        })
      }))


    ssc.start()
    ssc.awaitTermination()
    DbUtils.close()
  }
}
