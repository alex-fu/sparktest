name := "sparktest"

version := "1.0"

scalaVersion := "2.11.8"

val sparkV = "2.1.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql"
).map(_ % sparkV withSources())

transitiveClassifiers := Seq("sources")