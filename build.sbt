name := "query-engine"

version := "0.1"
scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1",
  "org.apache.spark" % "spark-yarn_2.11" % "2.1.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.1",
  "org.apache.spark" % "spark-hive_2.11" % "2.1.1",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.1",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.apache.hadoop" % "hadoop-client" % "2.7.7",
  "org.apache.hadoop" % "hadoop-common" % "2.7.7",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.7",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.0",
  "org.http4s" %% "http4s-blaze-server" % "0.15.4a",
  "org.http4s" %% "http4s-dsl"          % "0.15.4a",
  "org.http4s" %% "http4s-argonaut"     % "0.15.4a",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.6",
  "com.typesafe" % "config" % "1.3.1"
)

updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies ~= {_
  .map(_.exclude("javax.jms", "jms"))
  .map(_.exclude("com.sun.jdmk", "jmxtools"))
  .map(_.exclude("com.sun.jmx", "jmxri"))
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x if x.startsWith("META-INF") && x.endsWith(".SF") => MergeStrategy.discard
  case x if x.startsWith("META-INF") && x.endsWith(".RSA") => MergeStrategy.discard
  case x if x.startsWith("META-INF") && x.endsWith(".DSA") => MergeStrategy.discard
  case x if x.startsWith("META-INF") && x.endsWith(".TXT") => MergeStrategy.discard
  case x if x.startsWith("META-INF") && x.endsWith("org.apache.hadoop.fs.FileSystem") => MergeStrategy.concat
  case PathList(ps@_*) if ps.last endsWith ".conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

