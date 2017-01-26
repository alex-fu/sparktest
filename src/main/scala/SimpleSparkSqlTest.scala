import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Try}

case class Person(name: String, age: Long)

object SimpleSparkSqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SimpleSparkSqlTest")
      .getOrCreate()

    import spark.implicits._

    println("-" * 25 + "simple dataframe functions" + "-" * 25)
    val df = spark.read.json("/tmp/people.json")
    df.show()

    df.printSchema()

    df.select("name").show()

    df.select($"name", $"age" + 1).show()

    df.filter($"age" > 21).show()

    df.groupBy("age").count().show()

    println("-" * 25 + "test temp view" + "-" * 25)

    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    Try(spark.newSession().sql("SELECT * FROM people").show()) match {
      case Failure(e) => println(s"use temp view in new spark session failed: $e")
      case _ =>
    }

    println("-" * 25 + "test global temp view" + "-" * 25)

    df.createGlobalTempView("people")
    val sqlDFG = spark.sql("SELECT * FROM global_temp.people")
    sqlDFG.show()

    spark.newSession().sql("SELECT * FROM global_temp.people").show()


    println("-" * 25 + "test dataset" + "-" * 25)


    val personDS = spark.implicits.localSeqToDatasetHolder(Seq(Person("Andy", 32))).toDS()
    personDS.show()

    println("---")
    val primitiveDS = Seq(1, 2, 3)
    val a = primitiveDS.map(_ + 1)
    a.foreach{ x: Int =>
      println(x)
    }

    val path = "/tmp/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

    spark.stop()
  }
}
