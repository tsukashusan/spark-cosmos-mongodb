package example

import org.apache.log4j.{Level, Logger}
import com.mongodb.spark.sql._
import org.apache.spark.sql.{SQLContext, SparkSession}
import com.mongodb.spark._
import org.mongodb.scala.model.Aggregates._
import example.Helpers._

object SparkCosmosMongoDB extends Greeting with App {
  val rootLogger = Logger.getRootLogger().setLevel(Level.ERROR)
  var MONGO_DB:String = "<DB_NAME>"
  var MONGO_INPUT_COLLECTION:String = "<COLLECTION_NAME>"
  var MONGO_USERID:String = "<USER_ID>"
  var MONGO_READ_PASSWORD:String = "<INPUT_PASSOWRD>"
  var MONGO_READ_WRITE_PASSWORD:String = "<OUTPUT_PASSWORD>"
  var MONGO_HOST = "<HOST>"
  var MONGO_PORT = <PORT>

  val SPARK_SESSION = SparkSession.builder()
  .appName("MongoSparkConnectorIntro")
  .config("spark.mongodb.input.uri", s"mongodb://${MONGO_USERID}:${MONGO_READ_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${MONGO_DB}.${MONGO_INPUT_COLLECTION}?ssl=true&replicaSet=globaldb")
  .config("spark.mongodb.output.uri", s"mongodb://${MONGO_USERID}:${MONGO_READ_WRITE_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${MONGO_DB}.${MONGO_INPUT_COLLECTION}?ssl=true&replicaSet=globaldb")
  .getOrCreate()
  writeTest(SPARK_SESSION)
  //readTest(SPARK_SESSION)
  readUsingNative()
  SPARK_SESSION.sparkContext.setLogLevel("ERROR")

  def writeTest(sparkSession: SparkSession) : Unit = {
    import org.apache.spark.sql.SparkSession
    // For Self-Contained Scala Apps: Create the SparkSession 
    // CREATED AUTOMATICALLY IN spark-shell

    sparkSession.sparkContext.setLogLevel("INFO")

    import com.mongodb.spark._
    import com.mongodb.spark.config._
    import org.bson.Document

    val docs = """
      {"name": "Bilbo Baggins", "age": 50}
      {"name": "太郎 マイクロ", "age": 30}
      {"name": "花子 窓辺", "age": 27}
      {"name": "Balin", "age": 178}
      {"name": "Kíli", "age": 77}
      {"name": "Dwalin", "age": 169}
      {"name": "Óin", "age": 167}
      {"name": "Glóin", "age": 158}
      {"name": "Fíli", "age": 82}
      {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    sparkSession.sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB() 
      // Import the SQL helper

    println("##############complited###############")
  }

  def readTest(sparkSession: SparkSession) : Unit = {
    import org.apache.spark.sql.SparkSession
    import org.bson.Document
    val mongoRDD = sparkSession.sparkContext.loadFromMongoDB()
    val df = mongoRDD.toDF()
    //https://jira.mongodb.org/browse/SPARK-102
    //https://github.com/mongodb/mongo-spark/blob/master/src/main/scala/com/mongodb/spark/rdd/MongoRDD.scala
    df.show()
    println("############## aggregate4 ###############")
    println("############## aggregate ###############")
  }


  def readUsingNative() : Unit = {

      import scala.collection.JavaConverters._

      import com.mongodb.ServerAddress
      import com.mongodb.connection.netty.NettyStreamFactoryFactory
      import com.mongodb.async.client.MongoClientSettings
      import com.mongodb.connection.SslSettings
      import com.mongodb.MongoCredential
      import collection.JavaConversions._

      import io.netty.channel.nio.NioEventLoopGroup
      import io.netty.channel.EventLoopGroup

      import org.mongodb.scala.connection._
      import org.mongodb.scala._
      val user: String = MONGO_USERID // the user name
      val database: String = MONGO_DB // the name of the database in which the user is defined
      val password: Array[Char] = MONGO_READ_PASSWORD.toCharArray // the password as a character array

      val credential: MongoCredential = MongoCredential.createCredential(user, database, password)
      val eventLoopGroup: EventLoopGroup = new NioEventLoopGroup()
      val nettyStreamFactoryFactory: NettyStreamFactoryFactory =  NettyStreamFactoryFactory.builder
                                                                  .eventLoopGroup(eventLoopGroup)
                                                                  .build
      val clusterSettings: ClusterSettings = ClusterSettings.builder.hosts(List(new ServerAddress(s"${MONGO_HOST}:${MONGO_PORT}")).asJava).description("Local Server").build
      val settings: MongoClientSettings = MongoClientSettings.builder.clusterSettings(clusterSettings)
                                                                     .credentialList(List(credential))
                                                                     .streamFactoryFactory(nettyStreamFactoryFactory)
                                                                     .sslSettings(SslSettings.builder.enabled(true)
                                                                                                     .invalidHostNameAllowed(true)
                                                                                                     .build)
                                                                     .build
      // Create a codec for the Person case class
      import org.mongodb.scala.bson.codecs.Macros._
      import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
      import org.bson.codecs.configuration.CodecRegistries.{ fromRegistries, fromProviders }
      val codecRegistry = fromRegistries(fromProviders(classOf[Character]), DEFAULT_CODEC_REGISTRY)
      println("##########start connect##############")
      val client: MongoClient = MongoClient(settings)
      val mongoDbDatabase = client.getDatabase(MONGO_DB).withCodecRegistry(codecRegistry)
      val mongoDbCollection: MongoCollection[Document] = mongoDbDatabase.getCollection(MONGO_INPUT_COLLECTION)
      //mongoDbCollection.find.results.foreach(r => println(r.toJson))
      import org.mongodb.scala.bson._
      mongoDbCollection.aggregate(Seq(BsonDocument("""{$match:{"name":"Dwalin"}}""")))
                       .useCursor(false).results.foreach(r => println(r.toJson))
      //http://mongodb.github.io/mongo-scala-driver/2.2/scaladoc/org/mongodb/scala/AggregateObservable.html
      println("##########connected##############")
      eventLoopGroup.shutdownGracefully
      client.close
      SPARK_SESSION.close
      println("##########closed##############")
  }
  import org.mongodb.scala.bson.ObjectId
  case class Character(_id: ObjectId, name: String, age: Int)
}

trait Greeting {
  lazy val greeting: String = "hello"
}
