package exercices

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.col

import java.util.Properties
import scala.io.Source
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature._
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import Utils._

//goal: take a dataset of cyber indicator objects and use them to predict if they present a cyber threat or not
object DecisionTreeCorrected /*extends Serializable*/  {
  def main(args: Array[String]): Unit = {

    val csv_docs = "/home/houssem/scala-workspace/ML_BigDATA/IntrustionDATA/01/**"
    val spark = /*Utils.*/getSpark()

    import spark.sqlContext._
    val df = spark.read.option("header", true).csv(csv_docs).limit(1000)
    //df.printSchema()

/******************************cleaning***************************/
    val df_empty = /*MLUtils.*/checkEmptyResults(df)
    //df_empty.show(10, false)

    //based on df_empty content we will remove columns "dest_port" and "src_port"
    val remainingCols = df.columns.filterNot(col => (col.equals("dest_port") || col.equals("src_port"))).toList
    val df_notEmpty = df.select(remainingCols.map(col): _*)
    //df_notEmpty.printSchema()

    //sorting of columns --put label at the end
    var dimCols = new ListBuffer[String]()
    dimCols ++= (df_notEmpty.columns.filterNot(col => col.equals("label")).toList).view(2, 3)
    var sortedCols = ListBuffer[String]()
    sortedCols.++=(dimCols)
    sortedCols += "label"

    val df_clean = df_notEmpty.select(sortedCols.map(col): _*)
 
/******************************indexing***************************/
     val dimCols1=dimCols
      val df_converted=/*MLUtils.*/convertString_Double(df_clean,dimCols1)
    val df_inuse = /*MLUtils.*/indexLabel("label",dimCols1, df_converted)
    
    df_inuse.show(2,false)
   
/******************************classification***************************/
    //prepare training, test datasets
    val Array(trainingData, testData) = df_inuse.randomSplit(Array(0.7, 0.3))

    // Train a Decision Tree model.
    val rf = new DecisionTreeClassifier()
      .setLabelCol("label_indexed")
      .setFeaturesCol("feature_vector")
     // .setMaxBins(3000)
      //.setMaxDepth(10)

    
    
    /******************************classification***************************/
    
    val model = rf.fit(trainingData)
    val predictions = model.transform(testData)
    //val predictions=/*MLUtils.*/classify(rf,trainingData)

    //  val result= evaluateModel(rf, trainingData, testData)
    val result = /*MLUtils.*/evaluateModel(predictions)
    println(result)
  }
 def checkEmptyResults(df_in: DataFrame): DataFrame = {
	    var df_out = df_in.select(countCols(df_in.columns): _*)
	    df_out
	  }

	  //search columns having empty values in dataframe
	  def countCols(columns: Array[String]): Array[Column] = {
	    columns.map(c => {
	      count(when(col(c).isNull, c)).alias(c)
	    })
	  }
	  //index all categorical columns of a dataframe and return the indexed datafram
	  def indexDF(dimCols: ListBuffer[String], df_clean: DataFrame): DataFrame =
	    {
	      var indexers = new ListBuffer[StringIndexer]()
	      for (colName <- dimCols) {
	        val index = new StringIndexer()
	          .setInputCol(colName)
	          .setOutputCol(colName + "_indexed")
	        indexers += index
	      }
	      val pipeline = new Pipeline()
	        .setStages(indexers.toArray)
	      val df_indexed = pipeline.fit(df_clean).transform(df_clean)
	      var dimCols_index = new ListBuffer[String]()
	      dimCols_index ++= (df_indexed.columns.filter(col => col.contains("indexed")).toList)
	      val df_indexed2 = df_indexed.select(dimCols_index.map(col): _*)

	      //vector index
	      val assembler = new VectorAssembler()
	        .setInputCols(dimCols_index.filterNot(p => p.equals("label_indexed")).toArray)
	        .setOutputCol("feature_vector")
	      val df_inuse = assembler.transform(df_indexed2).select("feature_vector", "label_indexed")

	      df_inuse
	    }


	
	//index only label
	  def indexLabel(labelCol: String, dimCols: ListBuffer[String],df_clean: DataFrame): DataFrame =
	    {
	      var indexers = new ListBuffer[StringIndexer]()
	     
	        val index = new StringIndexer()
	          .setInputCol(labelCol)
	          .setOutputCol(labelCol + "_indexed")
	        indexers += index
	      
	      val pipeline = new Pipeline()
	        .setStages(indexers.toArray)
	      val df_indexed = pipeline.fit(df_clean).transform(df_clean)
	     

	      //vector index
	      val assembler = new VectorAssembler()
	        .setInputCols(dimCols.toArray)
	        .setOutputCol("feature_vector")
	      val df_inuse = assembler.transform(df_indexed).select("feature_vector", labelCol, labelCol+"_indexed")

	      df_inuse
	    }

	  
	    //evaluate the given model
	  def evaluateModel(predictions: DataFrame): String = {
	   

	    // obtain evaluator.
	    val evaluator = new MulticlassClassificationEvaluator()
	      .setLabelCol("label_indexed")
	      .setPredictionCol("prediction")
	      .setMetricName("accuracy")

	    // compute the classification error on test data.
	    val accuracy = evaluator.evaluate(predictions)

	    return s"Test Error = ${1 - accuracy}"
	  }
	  
	  
	  
	  //convert all colums present in "dimCOls" to doubleType
	 def convertString_Double(df_clean:DataFrame,dimCols:ListBuffer[String]):DataFrame=
	 {
	   
	   var df_converted=df_clean
	   var col_st=""
//	   for (  col_st <- dimCols)
//	   {
	   dimCols.foreach(col_st=>	   {  df_converted=df_converted.withColumn(col_st,col(col_st).cast("double"))
	   })
	   df_converted
	 }
	 
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
