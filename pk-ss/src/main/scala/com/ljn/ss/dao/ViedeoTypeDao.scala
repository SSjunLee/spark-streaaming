package com.ljn.ss.dao

import com.ljn.ss.util.DbUtils
import com.mongodb.client.model.Filters
import org.bson.Document

object ViedeoTypeDao {

  def save(recordList: Iterator[(String, Int)], conf: DbUtils.DbConfig)={
    val (col, client) = DbUtils.getCol("tb_type", conf)
    recordList.foreach(record => {
      val cond = Filters.eq("video_type", record._1)
      val kv = new Document("cnt", record._2)
      DbUtils.inc(cond, kv, col)
    })
    client.close()
  }
}
