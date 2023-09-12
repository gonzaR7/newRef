package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate


object cambio_precio extends SparkSessionWrapper  {

  def CalcularDataFrame(df_movimientos: DataFrame , df_costo_unificado: DataFrame , df_stock: DataFrame, df_articulos: DataFrame, fecha_inicial: String, fecha_final: String) : DataFrame = {

    // Definir una ventana ordenada por la columna "fecha_vigencia_desde" para cada "codigo_barras"
    val ventana = Window.partitionBy("barras").orderBy("fecha_vigencia_desde")
    // Costo unificado y en cada fila su costo anterior
    val df_costo_unificado_con_anterior = df_costo_unificado.withColumn("costo_anterior", when(lag("costo", 1).over(ventana).isNull, col("costo")).otherwise(lag("costo", 1).over(ventana)))
    val df_costo_unificado_periodo=df_costo_unificado_con_anterior.filter(f"fecha_vigencia_desde BETWEEN ${fecha_inicial} AND ${fecha_final}")
    val df_sin_mov_cu_temp=df_costo_unificado_periodo.as("cu").join(df_movimientos,(df_costo_unificado_periodo("barras")===df_movimientos("codigo_barra")),"left_outer").select(col("cu.*"),col("codigo_articulo").as("codigo")).withColumnRenamed("fecha_vigencia_desde","fecha_stock")
    // Join con articulos para agregar Codigo.
    val df_sin_mov_cu=df_sin_mov_cu_temp.as("cu").join(df_articulos,(df_sin_mov_cu_temp("barras")===df_articulos("codigo_barra")),"inner").select(col("cu.*"),col("codigo"))
    // Se filtra el stock para el día de hoy (ya que al haber movimientos, se realizó el cálculo para tener el nuevo stock del día)
    val df_stock_filtrado = df_stock.filter(col("fecha_stock") <= fecha_final)
    // Se agrupa el stock del día y se trae la fecha más reciente (stock actual)
    val df_stock_agrupado = df_stock_filtrado.groupBy("codigo").agg(max("fecha_stock").as("max_fecha_stock"))//.orderBy(desc("max_fecha_stock"))
    // Joineamos la fecha más actual con el stock, para traer el valor de existencia junto con el código y la fecha_stock
    val df_join_stock = df_stock_agrupado.join(df_stock, Seq("codigo"), "inner").filter(col("fecha_stock") === col("max_fecha_stock")).select(col("codigo"), col("existencia"), col("fecha_stock"))
    // Se joinea el stock actual junto con los artículos con movimientos y sus precios, a su vez se calcula RxT
    val df_pre_final: DataFrame = df_join_stock.as("stock").join(df_sin_mov_cu.as("precios"), Seq("codigo"), "inner").withColumn("movimientos_agrupados", lit(0)).withColumn("resultado_por_tenencia", ((col("costo_unitario") - col("costo_anterior")) * col("existencia"))).select(col("codigo"), col("barras"), col("precios.fecha_stock"), col("movimientos_agrupados"), col("stock.existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia")).distinct

    df_pre_final
  }
}