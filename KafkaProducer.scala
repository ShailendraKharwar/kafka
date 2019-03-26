package com.kafstream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    //creating spark session in local mode
    val spark = SparkSession.builder().appName("Ingestion").master("local[*]").getOrCreate()
    //creating schema for 2 Int columns
    val schema = StructType(
      List(
        StructField("equipmentPrice", IntegerType, true),
          StructField("rawMaterialPrice", IntegerType, true)
      )
    )
    //reading stream of data from local directory
    val data = spark
      .readStream.option("header","true")
      .option("delimiter",",")
      .schema(schema)
      .csv("file:///home/shailu/bigdata/input")

    //writing stream data to kafka topic in json format
    val finalData = data
      .select(to_json(struct(col("equipmentPrice"), col("rawMaterialPrice"))).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic","testing_stream")
      .option("checkpointLocation","file:///home/shailu/bigdata/sparkcheckpointproducer")
      .start.awaitTermination()
  }
}


