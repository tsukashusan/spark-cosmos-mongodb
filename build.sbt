import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.12",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "spark-cosmos-mongodb",
    resolvers ++= Seq("hortonworks release" at "http://repo.hortonworks.com/content/repositories/",
                      "hortonworks public" at "http://repo.hortonworks.com/content/groups/public/"),
    libraryDependencies  ++= Seq(scalaTest % Test,
                                spakCore,
                                spakSQL,
                                spakStreaming,
                                spakMlib,
                                sparkMongo,
                                scalaMongoDriver),
  )
assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case "application.conf"                            => MergeStrategy.concat
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".xml" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".types" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".conf" => MergeStrategy.concat
    case "unwanted.txt"                                => MergeStrategy.discard
    case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")