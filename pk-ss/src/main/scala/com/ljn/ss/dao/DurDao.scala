package com.ljn.ss.dao

import com.ljn.ss.util.DbUtils
import com.mongodb.client.model.Filters
import org.bson.Document

object DurDao {

  def save(recordList: Iterator[(Int, Int)], conf: DbUtils.DbConfig)={
    val (col, client) = DbUtils.getCol("tb_dur", conf)
    recordList.foreach(record => {
      val cond = Filters.eq("dur_type", record._1)
      val kv = new Document("cnt", record._2)
      DbUtils.inc(cond, kv, col)
    })
    client.close()
  }
}
