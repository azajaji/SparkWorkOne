

name := "LSHTimeSeries"
version := "1.0"
//scalaVersion := "2.12.2"
scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.11"  % "2.1.1" ,
  "org.apache.spark"  % "spark-mllib_2.11" % "2.1.1"
)

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.4",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4"
)

libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "2.8.2"

libraryDependencies += "com.esotericsoftware" % "kryo" % "4.0.0"
