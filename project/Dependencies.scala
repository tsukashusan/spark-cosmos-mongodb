import sbt._

object Dependencies {
   lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4"
   lazy val spakCore = "org.apache.spark" %% "spark-core" % "2.1.1.2.6.2.8-3" % "provided" exclude("org.apache.zookeeper", "zookeeper")
   lazy val spakSQL = "org.apache.spark" %% "spark-sql" % "2.1.1.2.6.2.8-3" % "provided" exclude("org.apache.zookeeper", "zookeeper")
   lazy val spakStreaming = "org.apache.spark" %% "spark-streaming" % "2.1.1.2.6.2.8-3" % "provided" exclude("org.apache.zookeeper", "zookeeper")
   lazy val spakMlib = "org.apache.spark" %%  "spark-mllib" % "2.1.1.2.6.2.8-3" % "provided" exclude("org.apache.zookeeper", "zookeeper")
   lazy val sparkMongo = "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.1"
   lazy val scalaMongoDriver = "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"
 }
