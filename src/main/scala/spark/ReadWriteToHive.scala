package com.subrata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.HashPartitioner

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._

object ReadWriteToHive {

  case class Csv(col1: String, col2: String, col3: String, col4: String)

  def createCsvData(line: String): Csv = {
    val fields = line.split(",")
    val col1 = fields(0).toString()
    val col2 = fields(1).toString()
    val col3 = fields(2).toString()
    val col4 = fields(3).toString()

    var csv: Csv = Csv(col1, col2, col3, col4)
    return csv
  }

  def main(args: Array[String]) {
    val bucketName = "subrata-csv";
    val key = "AKIAIUGNH2LOSBKNFHWA";
    val secret = "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";

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("writetohive")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._
    import sparkSession.sql

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", key)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret)

    val request = new ListObjectsRequest()
    request.setBucketName(bucketName)

    def client = new AmazonS3Client(new BasicAWSCredentials(key, secret))
    val allFiles = client.listObjects(request).getObjectSummaries.listIterator()
    val saveLoc = "s3n://subrata-db-csv/dir/"
    val tableName = "data15"

    while (allFiles.hasNext()) {
      val file = "s3n://" + bucketName + "/" + allFiles.next().getKey
      println("************* Result :" + file)

      val lines = sparkSession.sparkContext.textFile(file)
      val csv = lines.map(createCsvData)
      val df = csv.toDF.cache().coalesce(5)
      df.write.mode(SaveMode.Append).saveAsTable(tableName)
      //df.write.mode(SaveMode.Append).partitionBy("col2").saveAsTable(tableName)
      //df.write.mode(SaveMode.Append).save(saveLoc)
    }

    sql("Select * FROM " + tableName).show()

    sparkSession.stop()
  }

}