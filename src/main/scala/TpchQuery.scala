package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql._

import scala.collection.mutable.ListBuffer

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(sc: SparkContext, schemaProvider: TpchSchemaProvider, queryNum: Int): ListBuffer[(String, Float)] = {

    // if set write results to hdfs, if null write to stdout
    val OUTPUT_DIR: String = ""

    // val OUTPUT_DIR: String = "/tpch"
    // val OUTPUT_DIR: String = "file://" + new File(".").getAbsolutePath() + "/dbgen/output"

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]

      outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)

    }

    return results
  }

  def runBenchmark(conf: SparkConf, INPUT_DIR: String, queryNum: Int, numIter: Int): ListBuffer[(String, Float)] = {
    val sc = new SparkContext(conf)
    val schemaProvider = new TpchSchemaProvider(sc, INPUT_DIR)

    val output = new ListBuffer[(String, Float)]

    for (_ <- 1 to numIter) {
      output ++= executeQueries(sc, schemaProvider, queryNum)
    }

    output
  }

  def main(args: Array[String]): Unit = {

    var queryNum = 0;
    var numIter = 1;
    var sf = 1;
    var inputPath = ""
    if (args.length > 0) {
      queryNum = args(0).toInt
      numIter = args(1).toInt
      sf = args(2).toInt
      inputPath = "/input/sf" + args(2)
    }

    // read files from local FS
    // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen" + inputPath

    // read from hdfs
    val INPUT_DIR: String = "/dbgen" + inputPath

    val conf = new SparkConf()
        .setAppName("TPC-H")

    val output = runBenchmark(conf, INPUT_DIR, queryNum, numIter)

    val fileName = "TIMES"
    val outFile = new File("/home/ubuntu/spark-logs/" + fileName)
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}
