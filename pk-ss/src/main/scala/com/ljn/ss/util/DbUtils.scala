package com.ljn.ss.util

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, UpdateOptions}
import org.bson.Document


object DbUtils {

  import com.mongodb.MongoClient
  import com.mongodb.client.MongoDatabase

  val mongoClient = new MongoClient("vm", 27017)
  //连接数据库
  val db: MongoDatabase = mongoClient.getDatabase("ljn") //test可选

  def getCol()={
      db.getCollection("wc")
  }
  def close()={
     mongoClient.close()
  }

  def inc(word:String,v:Int,col: MongoCollection[Document])={
    col.updateOne(Filters.eq("word",word),new Document("$inc",new Document(
      "cnt",v
    )),new UpdateOptions().upsert(true))
  }

  def main(args: Array[String]): Unit = {

      val col =DbUtils.getCol()
      DbUtils.inc("abc",666,col)
      DbUtils.close()
  }

}
