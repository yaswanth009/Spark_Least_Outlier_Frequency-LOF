name := "Spark_Least_Outlier_Frequency_(LOF)"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-hive" % "2.2.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "org.apache.spark" %% "spark-catalyst" % "2.2.0" % Test,
  "org.apache.spark" %% "spark-yarn" % "2.2.0" % "provided"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}