package util;



import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.DeleteResult;


//bili@103.47.82.178

public enum MongoUtil {
    /**
     * 定义一个枚举的元素，它代表此类的一个实例
     */
    instance;

    private static MongoClient mongoClient;

    public final   static String DEFALUT_DBNAME = "bili";

    static {
        System.out.println("===============MongoDBUtil初始化========================");
        String ip = "103.47.82.178";
        int port =27017;
        instance.mongoClient = new MongoClient(ip, port);
        // 大部分用户使用mongodb都在安全内网下，但如果将mongodb设为安全验证模式，就需要在客户端提供用户名和密码：
        // boolean auth = db.authenticate(myUserName, myPassword);
        Builder options = new MongoClientOptions.Builder();
        options.cursorFinalizerEnabled(true);
        // options.autoConnectRetry(true);// 自动重连true
        // options.maxAutoConnectRetryTime(10); // the maximum auto connect retry time
        options.connectionsPerHost(300);// 连接池设置为300个连接,默认为100
        options.connectTimeout(30000);// 连接超时，推荐>3000毫秒
        options.maxWaitTime(5000); //
        options.socketTimeout(0);// 套接字超时时间，0无限制
        options.threadsAllowedToBlockForConnectionMultiplier(5000);// 线程队列数，如果连接线程排满了队列就会抛出“Out of semaphores to get db”错误。
        options.writeConcern(WriteConcern.SAFE);//
        options.build();
    }




    // ------------------------------------共用方法---------------------------------------------------
    /**
     * 获取DB实例 - 指定DB
     *
     * @param dbName
     * @return
     */
    public static MongoDatabase getDB(String dbName) {
        if (dbName != null && !"".equals(dbName)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            return database;
        }
        return null;
    }


    public static void inc(Object tp,int v,String col,String field)
    {
        MongoCollection<Document> coll
                = getDB(DEFALUT_DBNAME).getCollection(col);
        coll.updateOne(eq(field,tp),new Document("$inc",
                new Document("cnt",v)),new UpdateOptions().upsert(true));
    }


    public static void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    public static void main(String[]args){

    }


}