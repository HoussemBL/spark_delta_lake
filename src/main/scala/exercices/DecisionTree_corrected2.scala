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
object DecisionTreeCorrected2 extends Serializable {
  def main(args: Array[String]): Unit = {

    val csv_docs = "/home/houssem/scala-workspace/ML_BigDATA/IntrustionDATA/01/**"
    //val spark = /*Utils.*/getSpark()
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkML")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .config("spark.executor.memory", "50g")
      .config("spark.driver.memory", "50g")
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "10g")
      .getOrCreate()

    import spark.sqlContext._
    val df = spark.read.option("header", true).csv(csv_docs).limit(100)
    //df.printSchema()

/******************************cleaning***************************/

    //based on df_empty content we will remove columns "dest_port" and "src_port"
    val remainingCols = df.columns.filterNot(col => (col.equals("dest_port") || col.equals("src_port"))).toList
    val df_notEmpty = df.select(remainingCols.map(col): _*)
    //df_notEmpty.printSchema()

    //sorting of columns --put label at the end
    var dimCols = new ListBuffer[String]()
    dimCols ++= (df_notEmpty.columns.filterNot(col => col.equals("label")).toList).view(2, 5)
    var sortedCols = ListBuffer[String]()
    sortedCols.++=(dimCols)
    sortedCols += "label"

    val df_clean = df_notEmpty.select(sortedCols.map(col): _*)

/******************************converting to float***************************/
    val dimCols1 = dimCols
    //val df_converted=/*MLUtils.*/convertString_Double(df_clean,dimCols1)

    var df_converted = df_clean
    dimCols1.foreach(col_st => {
      df_converted = df_converted.withColumn(col_st, col(col_st).cast("double"))
    })

/*******************indexing*******************/
    //  val df_inuse = /*MLUtils.*/indexLabel("label",dimCols1, df_converted)
/*******************indexing*******************/
    val labelCol = "label"
   

    val index = new StringIndexer()
      .setInputCol(labelCol)
      .setOutputCol(labelCol + "_indexed")
    // indexers += index

    /*val pipeline = new Pipeline()
	        .setStages(indexers.toArray)
	      val df_indexed = pipeline.fit(df_converted).transform(df_converted)*/
    val df_indexed = index.fit(df_converted).transform(df_converted)

    //vector index
    val assembler = new VectorAssembler()
      .setInputCols(dimCols.toArray)
      .setOutputCol("feature_vector")
    val df_inuse = assembler.transform(df_indexed).select("feature_vector", labelCol, labelCol + "_indexed")

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

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label_indexed")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    // compute the classification error on test data.
    val accuracy = evaluator.evaluate(predictions)

    return s"Test Error = ${1 - accuracy}"

  }

}
