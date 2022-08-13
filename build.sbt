ThisBuild / version := "0.0.1"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "minimal-spark-jobs"
  )

logLevel := Level.Error
resolvers ++= Seq(
  "mvn" at "https://mvnrepository.com/artifact"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-streaming" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" % "spark-streaming-kinesis-asl_2.13" % "3.3.0",
  // 互換性のために最新のパッケージをいれておく
  "com.amazonaws" % "amazon-kinesis-client" % "1.14.8",
  "com.amazonaws" % "aws-java-sdk-kinesis" % "1.12.280"
)
