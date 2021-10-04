package com.datalake

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.log4j.{Level, Logger}

import Utils._

object DeltaProcessing /*extends Serializable*/  {
  def main(args: Array[String]): Unit = {

    val csv_docs = "/home/houssem/scala-workspace/ML_BigDATA/Grades.csv"
    val spark = Utils.getSpark()

    import spark.sqlContext._
    val df = spark.read.option("header", true).csv(csv_docs)
    //df.printSchema()

    
    //Creating a table
  df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("file:/home/houssem/delta-table/grades")

  
   //Reading a table
  val df1 = spark.read.format("delta").load("file:/home/houssem/delta-table/grades")
  df1.show(5,false)
  
  
  
   //Adding column to table
  val newDF = df.withColumn("University",lit("MIT"))

       //Storing in format delta
  newDF.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("file:/home/houssem/delta-table/grades")
   val df2 = spark.read.format("delta").load("file:/home/houssem/delta-table/grades")
  df2.show(5,false)
  
  
     //review several previous version of my dataframe
    val timeTravelDF_1 = spark.read.format("delta").option("versionAsOf", 0).load("file:/home/houssem/delta-table/grades")
  timeTravelDF_1.show(5,false)
//  
     val timeTravelDF_2 = spark.read.format("delta").option("versionAsOf", 1).load("file:/home/houssem/delta-table/grades")
  timeTravelDF_2.show(5,false)
  }
 
}
