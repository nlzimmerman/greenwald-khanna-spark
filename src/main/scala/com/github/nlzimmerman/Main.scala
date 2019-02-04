package com.github.nlzimmerman
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD}
import org.apache.log4j.{Level, Logger}
import scala.util.Random
import java.io.{
  BufferedWriter,
  OutputStreamWriter,
  FileOutputStream,
  BufferedReader,
  InputStreamReader,
  FileInputStream
}
import java.nio.charset.Charset
import scala.collection.mutable.ListBuffer

object Main extends App {
  import org.apache.spark.SparkContext._
  import com.github.nlzimmerman._
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  lazy val spark: SparkSession = {
    SparkSession.
    builder().
    master("local[12]").
    appName("example").
    getOrCreate()
  }

  import spark.sqlContext.implicits._

  /* Code that does something would go here */

  println("Hello world!")

  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}
