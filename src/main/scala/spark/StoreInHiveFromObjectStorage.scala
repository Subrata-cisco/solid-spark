package com.subrata.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.yarn.webapp.example.HelloWorld
import org.apache.spark.sql.SparkSession


object StoreInHiveFromObjectStorage {
  
  case class Csv(age:String,no:String)
  
  def mapper(line:String) :Csv = {
    val fields = line.split(",")
    val age = fields(0).toString()
    val numFriends = fields(1).toString()
    
    var csv:Csv = Csv(age,numFriends)
    return csv
  }
  

  def main(args: Array[String]) {
     //val hiveHost = "thrift://localhost:9083"
     val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("writetohive")
      //.config("hive.metastore.warehouse.dir", hiveHost + "user/hive/warehouse")
      //.config("spark.sql.warehouse.dir","/user/hive/warehouse")
      //.config("spark.hadoop.hive.metastore.warehouse.dir","C://Users//sususaha.ORADEV//workspace_scala//wh")
      //.config("hive.metastore.uris",hiveHost)
      .config("spark.sql.warehouse.dir","file:///C:/temp")
      .enableHiveSupport()
      .getOrCreate()
    
    val lines = sparkSession.sparkContext.textFile("../fakefriends.csv") 
    val csv = lines.map(mapper)

    /*import sparkSession.implicits._ 
    val ds = csv.toDS.cache().coalesce(5)
    ds.printSchema()
    ds.createTempView("csv")
    val teenagers = sparkSession.sql("SELECT * FROM csv WHERE age >= 13 AND age <= 19")
    val results = teenagers.collect()
    results.foreach(println)*/
    
    
    
    import sparkSession.implicits._
    import sparkSession.sql

    val df = csv.toDF.cache().coalesce(5)
    //sql("CREATE TABLE IF NOT EXISTS helloworld (key String, value String) USING hive")
    //df.write.mode(SaveMode.Append).insertInto("helloworld")
    
    df.write.mode(SaveMode.Append).saveAsTable("data1")
    
    
    sql("SELECT * FROM data1").show()
    
    sparkSession.stop()
  }

}