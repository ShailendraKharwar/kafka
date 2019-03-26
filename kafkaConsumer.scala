package com.kafstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType}

object kafkaConsumer {
  def main(args: Array[String]): Unit = {
    //creating spark session in local mode
    val spark = SparkSession.builder().appName("consumer").master("local[*]").getOrCreate()
    import spark.implicits._
    //creating schema for 2 Int columns
    val schema = (new StructType())
      .add("equipmentPrice",IntegerType)
      .add("rawMaterialPrice",IntegerType)
    //reading the stream from kafka topic
    val data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","testing_stream")
      .option("startingOffsets","earliest").option("group.id","stream_Group")
      .load

    //defining UDF
    def sumUdf = udf {(a: Int,b: Int) => a+b}
    //writing stream data from kafka topic to console
    val finalData = data
      .selectExpr("cast(value as string) as json")
      .select(from_json($"json", schema).as("data"))
      .select(col("data.equipmentPrice"),col("data.rawMaterialPrice"), sumUdf(col("data.equipmentPrice"),col("data.rawMaterialPrice")).as("sum"))
      .writeStream.outputMode("append")
      .format("console" ).option("checkpointLocation","file:///home/shailu/bigdata/sparkcheckpointConsumer")
      .start()
      .awaitTermination()

    println("finished")

  }
}