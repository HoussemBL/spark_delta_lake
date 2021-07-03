package Utils




import java.io.Serializable;
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
import org.apache.spark.ml.regression.LinearRegression

import org.apache.spark.ml.classification.Classifier



//contains all functions necessary to implement a learning model
object MLUtils /*extends Serializable*/{
	  //return a dataframe with percentage of empty columns
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
	
	
	
//	//fit the given machine learning model using the given training dataset
//	def classify(rf: Estimator,trainingData:DataFrame): DataFrame =
//	  {
//	  
//	  val model=rf.fit(trainingData)
//	  
//	   // Make predictions.
//          val predictions = model.transform(testData)
//	  
//	  predictions
//	  }
	
	
	  
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
	   dimCols.foreach(col_st=>	   {  df_converted=df_converted.withColumn(col_st,col(col_st).cast("double"))
	   })
	   df_converted
	 }
}
