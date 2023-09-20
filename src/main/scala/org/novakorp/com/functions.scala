package org.novakorp.com
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead, lit, when,round}


object functions extends SparkSessionWrapper {

  //Ingesta a la tabla
  def saveCurrentDF(df: DataFrame, outputTable: String): Unit = {
    df.write
      .mode("overwrite")
      .option("hive.exec.dynamic.partition", "true")
      .option("hive.exec.dynamic.partition.mode", "nonstrict")
      .insertInto(outputTable)
  }

  def agregarCostoAnterior(df: DataFrame): DataFrame = {
    val ventana = Window.partitionBy("barras").orderBy("fecha_vigencia_desde","id_costo_unificado")

    val df_costo_unificado_con_anterior = df.withColumn("costo_anterior", when(lag("costo", 1).over(ventana).isNull, col("costo")).otherwise(lag("costo", 1).over(ventana)))
    df_costo_unificado_con_anterior
  }

  def calcularResultadoPorTenencia(df:DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val windowStock = Window.partitionBy("codigo").orderBy("fecha_stock")

    val dfLead = df.withColumn("precio_siguiente", lead("costo_unitario", 1).over(windowStock)).withColumn("precio_anterior_siguiente", lead("costo_anterior", 1).over(windowStock)).withColumn("resultado_por_tenencia", when(col("costo_unitario") =!= col("precio_siguiente"),round((col("precio_siguiente") - col("precio_anterior_siguiente")) * col("total_unidades"),2)).otherwise(lit(0))).drop("stock_siguiente", "precio_siguiente", "precio_anterior_siguiente")
    dfLead
  }
}
