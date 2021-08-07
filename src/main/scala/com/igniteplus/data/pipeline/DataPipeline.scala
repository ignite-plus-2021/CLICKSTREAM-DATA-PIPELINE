package com.igniteplus.data.pipeline

import org.apache.spark.sql.SparkSession


object DataPipeline {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder().master("local[*]").appName("DataPipeline").getOrCreate()
    println("Hello")

  }
}
