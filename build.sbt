name := "spark-streaming-kafka-example"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0"
