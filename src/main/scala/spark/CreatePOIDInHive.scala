package com.subrata.spark

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object CreatePOIDInHive {
  
  case class hiveData(eventId: String,otherData: String, poid: String)

  def createHiveData(line: String): hiveData = {
    val fields = line.split("\\s+")
    
    val col1 = fields(0).toString()
    var col2 = ""
    var poid  = "0";
    
    if(fields.length > 1){
      col2 = fields(1).toString()
      val othetDataFields = col2.split(";")
      for(field <- othetDataFields){
        if(field.startsWith("u9")){
          poid = field.split("=")(1)
        }
      }
    }
    
    //println("*********** line found as  col1 :"+col1+" col2 :"+col2+" poid :"+poid)

    var data: hiveData = hiveData(col1, col2, poid)
    return data 
  }
  
  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("writetohive")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .enableHiveSupport()
      .getOrCreate()
      
    import sparkSession.implicits._ 
    import sparkSession.sql
      
    val u9File = "C:\\Users\\sususaha.ORADEV\\Desktop\\work\\bhargav\\u9.csv"; 
    val tableName = "poidTable1"
    
    val lines = sparkSession.sparkContext.textFile(u9File)
    val hiveData = lines.map(createHiveData) 
    
    val df = hiveData.toDF.cache().coalesce(5)  
    df.write.mode(SaveMode.Append).saveAsTable(tableName)

    sql("SELECT * FROM "+tableName).show()
    
    sparkSession.stop()
  }

  
}