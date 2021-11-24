package com.ljn.ss.manager

import com.ljn.ss.util.DbUtils
import com.mongodb.client.model.Filters
import org.apache.kafka.common.TopicPartition
import org.bson.Document

object MongodbOffserManager extends OffsetManager {
  val TBNAME = "tb_topic"

  override def storeOffsets(topic: String, partition: Int, offset: Long, dbConfig: DbUtils.DbConfig): Unit = {
    val (col,con) = DbUtils.getCol(TBNAME, dbConfig)
    val conds = Filters.and(Filters.eq("topic", topic), Filters.eq("partition", partition))
    val document = new Document()
    document.put("topic", topic)
    document.put("partition", partition)
    document.put("offset", offset)
    DbUtils.set(conds, document, col)
    con.close()
  }

  override def obtainOffsets(topic: String, partition: Int, dbConfig: DbUtils.DbConfig) = {
    val (col,con) = DbUtils.getCol("tb_topic", dbConfig)
    val offsetIt = DbUtils.find( Filters.eq("topic","test"),col)
    var offset = 0L
    if(offsetIt != null)offset = offsetIt.get("offset").asInstanceOf[Long]
    val partitionToLong:collection.Map[TopicPartition, Long] = collection.Map[TopicPartition, Long](
      new TopicPartition("test", 0) -> offset
    )
    partitionToLong
  }
}
