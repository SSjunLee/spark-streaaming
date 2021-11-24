package com.ljn.ss.util


import com.mongodb.MongoCredential
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, UpdateOptions}
import org.bson.Document
import org.bson.conversions.Bson


object DbUtils {

  import com.mongodb.MongoClient
  import com.mongodb.client.MongoDatabase

  case class DbConfig(ip:String,port:Int,name:String)

  def getCol(colName:String,dbConfig: DbConfig)={
    val mongoClient =  new MongoClient(dbConfig.ip, dbConfig.port)
    val db= mongoClient.getDatabase(dbConfig.name)
    (db.getCollection(colName),mongoClient)
  }


  def inc(cond:Bson,kv:Document,col: MongoCollection[Document])={
    col.updateOne(cond,new Document("$inc",kv),new UpdateOptions().upsert(true))
  }

  def set(cond:Bson,kv:Document,col: MongoCollection[Document])={
    col.updateOne(cond,new Document("$set",kv),new UpdateOptions().upsert(true))
  }

  def find(cond:Bson,col:MongoCollection[Document])={
    col.find(cond).first()
  }

  def main(args: Array[String]): Unit = {
    val credential = MongoCredential.createScramSha1Credential("", "", "".toCharArray)

  }


}
