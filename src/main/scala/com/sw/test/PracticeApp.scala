package com.sw.test

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PracticeApp {

  val home: String = "/root/p1"
  val groups: List[String] = List("gender", "race/ethnicity", "parental level of education", "lunch", "test preparation course")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Practice1 Application").getOrCreate()
    val df = spark.read.format("csv").options(Map("header"->"true", "inferSchema"->"true")).load(s"$home/StudentsPerformance.csv")
    val cube = df.cube("gender", "race/ethnicity", "parental level of education", "lunch", "test preparation course")
      .agg(grouping_id(), mean("math score"), variance("math score"))

    // Q1
    getQ1Result(spark, cube)
    // Q2
    getQ2Result(cube)

    spark.stop()
  }

  def getQ1Result(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val res = df.filter($"grouping_id()" === 0)
      .select(
        $"gender", $"race/ethnicity", $"parental level of education", $"lunch", $"test preparation course",
        $"avg(math score)".as("mean"),
        $"var_samp(math score)".as("variance")
      )
      .sort("gender", "race/ethnicity", "parental level of education", "lunch", "test preparation course")

    writeToCsv(res, "q1")
  }

  def getQ2Result(df: DataFrame): Unit = {
    groups.foreach { target =>
      var filterExpr = col(target).isNotNull
      groups.filterNot(str => str == target).foreach(str => filterExpr &&= col(str).isNull)

      val res = df.filter(filterExpr)
        .select(col(target), col("avg(math score)").as("mean"))
        .sort(target)

      writeToCsv(res, s"q2/${target.replace("/", "_")}")
    }
  }

  def writeToCsv(df: DataFrame, name: String): Unit = {
    df.coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(s"$home/$name.csv")
  }
}
