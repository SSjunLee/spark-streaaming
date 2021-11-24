package com.ljn.ss

import com.ljn.ss.dao.{ViedeoTypeDao, DurDao}
import com.ljn.ss.manager.MongodbOffserManager
import com.ljn.ss.util.{ConfigUtil, DbUtils, RankUtils}
import com.mongodb.client.model.Filters
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document

object StreamApp {

  def getDbConfig(cu: ConfigUtil) = {
    DbUtils.DbConfig(cu.get(ConfigUtil.MONGO_IP), cu.get(ConfigUtil.MONGO_PORT).toInt, cu.get(ConfigUtil.MONGO_DBNAME))
  }

  def getKafkaStrategy(cu: ConfigUtil, dbConfig: DbUtils.DbConfig) = {
    val ip = cu.get(ConfigUtil.KAFKA_IP)
    val port = cu.get(ConfigUtil.KAFKA_PORT)
    val topic = cu.get(ConfigUtil.KAFKA_TOPIC)
    val partition = cu.get(ConfigUtil.KAFKA_PARTITION).toInt

    val serversStr = ip + ":" + port
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> serversStr,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pk-spark",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val partitionToOffset = MongodbOffserManager.obtainOffsets(topic, partition, dbConfig)
    val topics = Array(topic)
    Subscribe[String, String](topics, kafkaParams, partitionToOffset)
  }

  def initStreamingContext(cu: ConfigUtil, dbConfig: DbUtils.DbConfig) = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[RankUtils]))
    if (cu.get(ConfigUtil.ENV).equals("test"))
      sparkConf.setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val second = cu.get(ConfigUtil.INTERVAL).toInt
    val context = new StreamingContext(sparkConf, Seconds(second))
    val boradConfig = context.sparkContext.broadcast(dbConfig)
    val boardRankUtils = context.sparkContext.broadcast(new RankUtils())
    (context, boradConfig, boardRankUtils)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("input config path")
      System.exit(-1)
    }
    val cu = new ConfigUtil(args(0))
    val dbConfig = getDbConfig(cu)

    val (ssc, boardCastConfig, boardRankUtils) = initStreamingContext(cu, dbConfig)
    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      getKafkaStrategy(cu, dbConfig)
    )
    lines.foreachRDD(x => {
      val ranges = x.asInstanceOf[HasOffsetRanges].offsetRanges
      ranges.map(offsetRange => {
        MongodbOffserManager.storeOffsets(offsetRange.topic,
          offsetRange.partition,
          offsetRange.untilOffset,
          boardCastConfig.value)
        offsetRange
      })
    })
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
          ViedeoTypeDao.save(par,boardCastConfig.value)
        }
      )
    })
    type2dur.map(x => {
      val dur = x._2
      (boardRankUtils.value.getRank(dur.toInt), 1)
    }).reduceByKey(_ + _)
      .foreachRDD(x => x.foreachPartition(par => {
        DurDao.save(par,boardCastConfig.value)
      }))



    ssc.start()
    ssc.awaitTermination()

  }
}
