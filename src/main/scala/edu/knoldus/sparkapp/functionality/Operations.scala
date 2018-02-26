package edu.knoldus.sparkapp.functionality

import org.joda.time.DateTime
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

object Operations extends App {
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Spark Application")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val fileOne = sc.textFile("/home/knoldus/file1.txt")

  val fileTwo= sc.textFile("/home/knoldus/file2.txt")
  val fileOneRDD= fileOne.map(x => x.split("#")).map(x => (x(0),x(4))).toDF("id", "state")

  val fileTwoRDD= fileTwo.map(x => x.split("#")).map(x => (new DateTime(x(0).toLong * 1000L).toDateTime,x(1),x(2).toInt)).toDF("time","id", "sales")
  val finalFileTwoRDD=fileTwoRDD
    .withColumn("year", year(col("time")))
    .withColumn("month", month(col("time")))
    .withColumn("day", dayofmonth(col("time")))

  val partitionOne=finalFileTwoRDD.join(fileOneRDD,"id").groupBy( "year","id").sum( "sales" )
  val partitionTwo=finalFileTwoRDD.join(fileOneRDD,"id").groupBy( "year","month","id").sum( "sales" )
  val partitionThree= finalFileTwoRDD.join(fileOneRDD,"id").groupBy( "year","month","day","id").sum( "sales" )

  val finalPartitionOne=partitionOne
    .withColumn("month", lit(null: String))
    .withColumn("day", lit(null: String))
  val finalPartitionTwo=partitionTwo
    .withColumn("day", lit(null: String))

  val unionOne= finalPartitionOne.join(fileOneRDD,"id").select("state","year","month","day","sum(sales)")
  val unionTwo=   finalPartitionTwo.join(fileOneRDD,"id").select("state","year","month","day","sum(sales)")
  val unionThree= partitionThree.join(fileOneRDD,"id").select("state","year","month","day","sum(sales)")


  val inter=unionOne.union(unionTwo)
  val result=inter.union(unionThree)

   result.show()

}