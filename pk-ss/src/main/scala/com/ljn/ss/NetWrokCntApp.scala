package com.ljn.ss


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object NetWrokCntApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      //.setAppName(this.getClass.getSimpleName)
      //.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(8))

    val lines = ssc.socketTextStream("hadoop000", 9527)
    lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
