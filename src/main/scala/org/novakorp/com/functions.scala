package org.novakorp.com
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, when}


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

  def agregarCostoAnterior(df: DataFrame): DataFrame = {
    val ventana = Window.partitionBy("barras").orderBy("id_costo_unificado")

    val df_costo_unificado_con_anterior = df.withColumn("costo_anterior", when(lag("costo", 1).over(ventana).isNull, col("costo")).otherwise(lag("costo", 1).over(ventana)))
    df_costo_unificado_con_anterior
  }
}
