package org.novakorp.com
import org.apache.spark.sql.DataFrame


object functions extends SparkSessionWrapper {

  //Ingesta a la tabla
  def saveCurrentDF(df: DataFrame, outputTable: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("hive.exec.dynamic.partition", "true")
      .option("hive.exec.dynamic.partition.mode", "nonstrict")
      .insertInto(outputTable)
  }
}
