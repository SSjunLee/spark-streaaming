package com.ljn.ss.util

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, UpdateOptions}
import org.bson.Document
import org.bson.conversions.Bson


object DbUtils {

  import com.mongodb.MongoClient
  import com.mongodb.client.MongoDatabase

  var mongoClient:MongoClient = _//new MongoClient("vm", 27017)
  //连接数据库
  var db: MongoDatabase = _//mongoClient.getDatabase("ljn")


  def init(ip:String,port:Int,dbname:String)={
    mongoClient =  new MongoClient(ip, port)
    db= mongoClient.getDatabase(dbname)
  }

  def getCol(colName:String)={
      db.getCollection(colName)
  }
  def close()={
     mongoClient.close()
  }

  def inc(word:String,v:Int,col: MongoCollection[Document])={
    col.updateOne(Filters.eq("word",word),new Document("$inc",new Document(
      "cnt",v
    )),new UpdateOptions().upsert(true))
  }

  def inc(cond:Bson,kv:Document,col: MongoCollection[Document])={
    col.updateOne(cond,new Document("$inc",kv),new UpdateOptions().upsert(true))
  }

  def main(args: Array[String]): Unit = {


  }

}
