version := "1.0"

scalaVersion := "2.12.10" // Adjust according to your requirements

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
)

lazy val root = (project in file("."))
  .settings(
    name := "SparkDSL"
  )
