package com.subrata.spark

import org.apache.spark.sql.SparkSession

object ReadFromHiveTable {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("writetohive")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .enableHiveSupport()
      .getOrCreate()

    val key = "AKIAIVWJWIJJZR7CDLMQ";
    val secret = "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";

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", key)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret)

    /*val teenagers = sparkSession.sql("SELECT * FROM helloworld2 WHERE age >= 13 AND age <= 19")
    val results = teenagers.collect()
    results.foreach(println)*/

    import sparkSession.sql
    sql("SELECT * FROM data8").show()
    
     /*df
        .write
        .option("mode", "DROPMALFORMED")
        .option("compression", "snappy")
        .option("path", saveLoc)
        .mode(SaveMode.Append)
        //.format("parquet")
        .saveAsTable(tableName)*/
  

    //sql("CREATE EXTERNAL TABLE myTable (key STRING, value INT) LOCATION 's3n://subrata-db-csv/dir/'")

    sparkSession.stop()
  }

}