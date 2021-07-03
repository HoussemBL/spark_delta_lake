package exercices

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
import Utils._
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature._
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.regression.LinearRegression

import org.apache.spark.ml.classification.Classifier


//goal: take a dataset of cyber indicator objects and use them to predict if they present a cyber threat or not

//here we don't index numerical attribute
object LinearRegression extends Serializable {
  def main(args: Array[String]): Unit = {

    val csv_docs = "/home/houssem/scala-workspace/ML_BigDATA/IntrustionDATA/01/**"
    val spark = Utils.getSpark()

    import spark.sqlContext._
    val df = spark.read.option("header", true).csv(csv_docs).limit(10000)
    //df.printSchema()

/******************************cleaning***************************/
    val df_empty = MLUtils.checkEmptyResults(df)
    //df_empty.show(10, false)

    //based on df_emoty content we will remove columns "dest_port" and "src_port"
    val remainingCols = df.columns.filterNot(col => (col.equals("dest_port") || col.equals("src_port"))).toList
    val df_notEmpty = df.select(remainingCols.map(col): _*)
    //df_notEmpty.printSchema()

    //sorting of columns --put label at the end
    var dimCols = new ListBuffer[String]()
    dimCols ++= (df_notEmpty.columns.filterNot(col => col.equals("label")).toList).view(2, 5)
     /* var sortedCols = ListBuffer[String]()
    sortedCols.++=(dimCols)*/
    var sortedCols =dimCols
    sortedCols += "label"

    val df_clean = df_notEmpty.select(sortedCols.map(col): _*)

/******************************indexing***************************/


    val df_inuse = MLUtils.indexDF(dimCols,df_clean)

    //prepare training, test datasets
    val Array(trainingData, testData) = df_inuse.randomSplit(Array(0.7, 0.3))

 // Train a RandomForest model.
    val lr = new LinearRegression()
     .setLabelCol("label_indexed")
      .setFeaturesCol("feature_vector")
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

  

    // Train model. This also runs the indexers.
    val model = lr.fit(trainingData)

   // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.show(5)
    
       
    // obtain evaluator.
//val evaluator = new MulticlassClassificationEvaluator()
//     .setLabelCol("label_indexed")
//  .setMetricName("accuracy")
//
//// compute the classification error on test data.
//val accuracy = evaluator.evaluate(predictions)
//println(s"Test Error = ${1 - accuracy}")


    
    val result= MLUtils.evaluateModel(predictions)
      println(result)
  }

 
}
