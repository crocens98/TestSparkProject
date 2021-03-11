package by.zinkov

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()

      //.master("local[1]")
      .getOrCreate()


    spark.
      read.
      format("csv").
      option("header", "true").
      load("hdfs://localhost:9000/simple.csv")
      .write
      .format("avro")
      .save("hdfs://localhost:9000/avro-files")
  }
}
