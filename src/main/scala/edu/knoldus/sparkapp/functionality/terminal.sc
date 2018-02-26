 scala > val fileOne = sc.textFile("file1")
    fileOne: org.apache.spark.rdd.RDD[String] = file1 MapPartitionsRDD[1] at textFile at <console>:24

  scala>   val fileTwo = sc.textFile("file2")
  fileTwo: org.apache.spark.rdd.RDD[String] = file2 MapPartitionsRDD[3] at textFile at <console>:24

    scala>   val fileOneRDD= fileOne.map(x => x.split("#")).map(x => (x(0),x(4))).toDF("id", "state")
    fileOneRDD: org.apache.spark.sql.DataFrame = [id: string, state: string]
     scala> fileOneRDD.show()
     +------+-----+
     |    id|state|
     +------+-----+
     |   123|   CA|
     |   456|   AK|
     |   789|   AL|
     |101112|   OR|
     +------+-----+

     scala>   val fileTwoRDD= fileTwo.map(x => x.split("#")).map(x => (new DateTime(x(0).toLong * 1000L).toDateTime,x(1),x(2).toInt)).toDF("time","id", "sales")
    fileTwoRDD: org.apache.spark.sql.DataFrame = [time: string, id: string ... 1 more field]

    scala> val finalFileTwoRDD = (fileTwoRDD
    |    .withColumn("year", year(col("time")))
    |  .withColumn("month", month(col("time")))
    |   .withColumn("day", dayofmonth(col("time")))
    | )
    finalFileTwoRDD: org.apache.spark.sql.DataFrame = [time: string, id: string ... 4 more fields]

    scala> finalFileTwoRDD.show()
    +--------------------+---+------+----+-----+---+
    |                time| id| sales|year|month|day|
    +--------------------+---+------+----+-----+---+
    |2016-02-01T13:30:...|123|123456|2016|    2|  1|
    |2017-08-01T14:30:...|789|123457|2017|    8|  1|
    |2016-08-01T15:30:...|456|123458|2016|    8|  1|
    |2016-08-01T16:30:...|789|123459|2016|    8|  1|
    |2016-08-01T16:30:...|789|223459|2016|    8|  1|
    +--------------------+---+------+----+-----+---+


    scala> val partitionOne=finalFileTwoRDD.join(fileOneRDD,"id").groupBy( "year","id").sum( "sales" )
    partitionOne: org.apache.spark.sql.DataFrame = [year: int, id: string ... 1 more field]

     scala> partitionOne.show()
     +----+---+----------+
     |year| id|sum(sales)|
     +----+---+----------+
     |2017|789|    123457|
     |2016|789|    346918|
     |2016|456|    123458|
     |2016|123|    123456|
     +----+---+----------+

     scala>   val partitionTwo=finalFileTwoRDD.join(fileOneRDD,"id").groupBy( "year","month","id").sum( "sales" )
    partitionTwo: org.apache.spark.sql.DataFrame = [year: int, month: int ... 2 more fields]

     scala> partitionTwo.show()
     +----+-----+---+----------+
     |year|month| id|sum(sales)|
     +----+-----+---+----------+
     |2017|    8|789|    123457|
     |2016|    8|789|    346918|
     |2016|    8|456|    123458|
     |2016|    2|123|    123456|
     +----+-----+---+----------+

     scala>   val partitionThree= finalFileTwoRDD.join(fileOneRDD,"id").groupBy( "year","month","day","id").sum( "sales" )
    partitionThree: org.apache.spark.sql.DataFrame = [year: int, month: int ... 3 more fields]

     scala> partitionThree.show()
     +----+-----+---+---+----------+
     |year|month|day| id|sum(sales)|
     +----+-----+---+---+----------+
     |2017|    8|  1|789|    123457|
     |2016|    8|  1|789|    346918|
     |2016|    8|  1|456|    123458|
     |2016|    2|  1|123|    123456|
     +----+-----+---+---+----------+


     scala>  val finalPartitionOne = (partitionOne
    |  .withColumn("month", lit(null: String))
    |  .withColumn("day", lit(null: String))
    | )
    finalPartitionOne: org.apache.spark.sql.DataFrame = [year: int, id: string ... 3 more fields]

     scala> finalPartitionOne.show()
     +----+---+----------+-----+----+
     |year| id|sum(sales)|month| day|
     +----+---+----------+-----+----+
     |2017|789|    123457| null|null|
     |2016|789|    346918| null|null|
     |2016|456|    123458| null|null|
     |2016|123|    123456| null|null|
     +----+---+----------+-----+----+

     scala>   val finalPartitionTwo = (partitionTwo
    |    .withColumn("day", lit(null: String))
    | )
    finalPartitionTwo: org.apache.spark.sql.DataFrame = [year: int, month: int ... 3 more fields]
     scala> finalPartitionTwo.show()
     +----+-----+---+----------+----+
     |year|month| id|sum(sales)| day|
     +----+-----+---+----------+----+
     |2017|    8|789|    123457|null|
     |2016|    8|789|    346918|null|
     |2016|    8|456|    123458|null|
     |2016|    2|123|    123456|null|
     +----+-----+---+----------+----+


     scala> val unionOne= finalPartitionOne.join(fileOneRDD,"id").select("state","year","month","day","sum(sales)")
    unionOne: org.apache.spark.sql.DataFrame = [state: string, year: int ... 3 more fields]

     scala> unionOne.show()
     +-----+----+-----+----+----------+
     |state|year|month| day|sum(sales)|
     +-----+----+-----+----+----------+
     |   AL|2017| null|null|    123457|
     |   AL|2016| null|null|    346918|
     |   AK|2016| null|null|    123458|
     |   CA|2016| null|null|    123456|
     +-----+----+-----+----+----------+


    scala> val unionTwo=   finalPartitionTwo.join(fileOneRDD,"id").select("state","year","month","day","sum(sales)")
    unionTwo: org.apache.spark.sql.DataFrame = [state: string, year: int ... 3 more fields]

     scala> unionTwo.show()
     +-----+----+-----+----+----------+
     |state|year|month| day|sum(sales)|
     +-----+----+-----+----+----------+
     |   AL|2017|    8|null|    123457|
     |   AL|2016|    8|null|    346918|
     |   AK|2016|    8|null|    123458|
     |   CA|2016|    2|null|    123456|
     +-----+----+-----+----+----------+

     scala>  val unionThree= partitionThree.join(fileOneRDD,"id").select("state","year","month","day","sum(sales)")
     unionThree: org.apache.spark.sql.DataFrame = [state: string, year: int ... 3 more fields]


     scala> unionThree.show()
     +-----+----+-----+---+----------+
     |state|year|month|day|sum(sales)|
     +-----+----+-----+---+----------+
     |   AL|2017|    8|  1|    123457|
     |   AL|2016|    8|  1|    346918|
     |   AK|2016|    8|  1|    123458|
     |   CA|2016|    2|  1|    123456|
     +-----+----+-----+---+----------+


     scala> val inter=unionOne.union(unionTwo)
    inter: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [state: string, year: int ... 3 more fields]

    scala>   val result=inter.union(unionThree)
    result: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [state: string, year: int ... 3 more fields]

    scala> result.show()
    +-----+----+-----+----+----------+
    |state|year|month| day|sum(sales)|
    +-----+----+-----+----+----------+
    |   AL|2017| null|null|    123457|
    |   AL|2016| null|null|    346918|
    |   AK|2016| null|null|    123458|
    |   CA|2016| null|null|    123456|
    |   AL|2017|    8|null|    123457|
    |   AL|2016|    8|null|    346918|
    |   AK|2016|    8|null|    123458|
    |   CA|2016|    2|null|    123456|
    |   AL|2017|    8|   1|    123457|
    |   AL|2016|    8|   1|    346918|
    |   AK|2016|    8|   1|    123458|
    |   CA|2016|    2|   1|    123456|
    +-----+----+-----+----+----------+

