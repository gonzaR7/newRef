package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate


object con_movimientos extends SparkSessionWrapper  {

  //CON MOVIMIENTOS

  def CalcularDataFrame(df_con_mov_con_cu:DataFrame, df_movimientos: DataFrame , df_costo_unificado: DataFrame, df_stock: DataFrame,fecha_inicial: String, fecha_final: String): DataFrame = {

    // Se filtran los artículos que no tienen cambio de precio en el día
    val df_con_mov_filtrado: DataFrame = df_movimientos.join(df_con_mov_con_cu, df_movimientos("codigo_barra")===df_con_mov_con_cu("barras"), "left_outer")

    // Se agrupan los movimientos del día por codigo
    val df_mov_agrupado: DataFrame = df_con_mov_filtrado.groupBy("codigo_articulo", "fecha", "codigo_barra").agg(sum(col("cantidad_movimiento") * col("multiplicador_stock").cast("int")).as("movimientos_agrupados"))

    // Definir una ventana ordenada por la columna "fecha_vigencia_desde" para cada "codigo_barras"
    val ventana = Window.partitionBy("barras").orderBy("fecha_vigencia_desde")

    // Costo unificado y en cada fila su costo anterior
    val df_costo_unificado_con_anterior = df_costo_unificado.withColumn("costo_anterior", when(lag("costo", 1).over(ventana).isNull, col("costo")).otherwise(lag("costo", 1).over(ventana)))

    val joinedDF = df_mov_agrupado.join(df_costo_unificado_con_anterior,(df_mov_agrupado("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (df_costo_unificado_con_anterior("fecha_vigencia_desde") < df_mov_agrupado("fecha")),"inner").groupBy(df_mov_agrupado("codigo_barra")).agg(max(df_costo_unificado_con_anterior("fecha_vigencia_desde")).alias("max_fecha_vigencia"))

    val df_costo_unificado_actual = joinedDF.join(df_costo_unificado_con_anterior.as("cu"),(joinedDF("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (joinedDF("max_fecha_vigencia") === df_costo_unificado_con_anterior("fecha_vigencia_desde")),"inner").select(col("cu.*"))

    // Se joinean los costos actuales y anteriores con los artículos con movimientos y sin cambio de precio
    val df_precios: DataFrame = df_costo_unificado_actual.join(df_mov_agrupado, (df_costo_unificado_actual("barras") === df_mov_agrupado("codigo_barra")), "inner").select(col("codigo_articulo").as("codigo"), col("barras"), col("fecha").as("fecha_stock"), col("movimientos_agrupados"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))

    // Se filtra el stock para el día de hoy (ya que al haber movimientos, se realizó el cálculo para tener el nuevo stock del día)
    val df_stock_filtrado = df_stock.filter(f"fecha_stock BEETWEEN ${fecha_inicial} AND ${fecha_final}")

    // Se joinea el stock actual junto con los artículos con movimientos y sus precios, a su vez se calcula RxT
    val df_pre_final: DataFrame = df_stock_filtrado.as("stock").join(df_precios.as("precios"), Seq("codigo","fecha_stock"), "inner").withColumn("resultado_por_tenencia",lit(0)).select(col("codigo"), col("barras"), col("precios.fecha_stock"), col("movimientos_agrupados").as("movimientos_agrupados"), col("stock.existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia")).distinct

    df_pre_final

  }

}