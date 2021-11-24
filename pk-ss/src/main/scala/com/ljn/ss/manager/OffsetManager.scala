package com.ljn.ss.manager

import com.ljn.ss.util.DbUtils.DbConfig
import org.apache.kafka.common.TopicPartition

trait OffsetManager {
  // 入参：offsetRanges和groupid
  def storeOffsets(topic:String,partition:Int,offset:Long,dbConfig: DbConfig):Unit

  //入参：groupid和topic
  def obtainOffsets(topic:String,partition:Int,dbConfig: DbConfig):collection.Map[TopicPartition, Long]
}
