package com.ljn.ss


import com.ljn.ss.util.DbUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}




object DbWrodCntApp {



  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      //.setAppName(this.getClass.getSimpleName)
      //.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(8))

    val lines = ssc.socketTextStream("hadoop000", 9527)
    lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
      .foreachRDD(rdd=>{
        rdd.foreachPartition(par=>{
         val col = DbUtils.getCol()
          par.foreach(record=>{
            DbUtils.inc(record._1,record._2.toInt,col)
          })
        })
      })
    ssc.start()
    ssc.awaitTermination()
    DbUtils.close()
  }
}
