package Utils



import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//import stream._



object Utils{
  
  
     //get spark
  def getSpark():SparkSession={
    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkML")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
   .config("spark.executor.memory", "50g")
      .config("spark.driver.memory", "50g")
     .config("spark.memory.offHeap.enabled",true)
     .config("spark.memory.offHeap.size","10g")  
      .getOrCreate()
  spark    
  }
 
  
  
}