package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate


object con_movimientos extends SparkSessionWrapper  {

  //CON MOVIMIENTOS

  def CalcularDataFrame(/*df_con_mov_con_cu:DataFrame, */df_movimientos: DataFrame , df_costo_unificado: DataFrame, df_stock: DataFrame,fecha_inicial: String, fecha_final: String): DataFrame = {

    // Se filtran los artículos del día
    val df_con_mov: DataFrame = df_movimientos.filter(col("fecha") === fechaHoy)

    // Se filtran los artículos que no tienen cambio de precio en el día
    val df_con_mov_filtrado: DataFrame = df_con_mov.select("codigo_barra").except(df_con_mov_con_cu.select("barras"))

    // Traigo los artículos que sí tienen movimientos, pero que no están en la función de movimiento y cambio de precio
    val df_join_mov: DataFrame = df_con_mov_filtrado.join(df_con_mov, Seq("codigo_barra"), "inner")

    // Se agrupan los movimientos del día por codigo
    val df_mov_agrupado: DataFrame = df_join_mov.groupBy("codigo_articulo", "fecha", "codigo_barra").agg(sum(col("cantidad_movimiento") * col("multiplicador_stock").cast("int")).as("movimientos_agrupados"))

    // Se filtra costo_unificado para traer todos los registros con las fechas anteriores a la de hoy
    val df_cu_dia_menor: DataFrame = df_costo_unificado.filter(col("fecha_vigencia_desde") < fechaHoy)

    // Se agrupa por artículo y se trae la fecha más reciente de costo_unificado (van a ser los precios que vamos a usar en el df a ingestar)
    val df_cu_dia_costo_actual: DataFrame = df_cu_dia_menor.groupBy("barras").agg(max("fecha_vigencia_desde").as("fecha_mas_reciente"))

    // Se agrupa por artículo y se trae la 2da fecha más reciente de costo_unificado (va a ser nuestro costo_anterior)
    val df_cu_dia_costo_anterior: DataFrame = df_cu_dia_menor.join(df_cu_dia_costo_actual, Seq("barras")).where(col("fecha_vigencia_desde") < col("fecha_mas_reciente")).groupBy("barras").agg(max("fecha_vigencia_desde").as("fecha_anterior"))

    // Se joinea costo_unificado con la fecha más reciente para traer costo_unitario, precio_actual_mayorista y precio_actual_minorista
    val df_precio_actual: DataFrame = df_cu_dia_costo_actual.as("actual").join(df_costo_unificado.as("cu"), Seq("barras"), "inner").where(col("actual.fecha_mas_reciente") === col("cu.fecha_vigencia_desde")).select(col("barras").as("actual_barras"), col("costo").as("costo_unitario"), col("precio_minorista").as("precio_actual_minorista"), col("precio_mayorista").as("precio_actual_mayorista"))

    // Se joinea costo_unificado con la 2da fecha más reciente para traer costo_anterior
    val df_precio_anterior: DataFrame = df_cu_dia_costo_anterior.as("anterior").join(df_costo_unificado.as("cu"), Seq("barras"), "inner").where(col("anterior.fecha_anterior") === col("cu.fecha_vigencia_desde")).select(col("barras").as("anterior_barras"), col("costo").as("costo_anterior"))

    // Se joinean los df con los precios actuales y el df con costo_anterior
    val df_join_precios: DataFrame = df_precio_actual.join(df_precio_anterior, col("actual_barras") === col("anterior_barras"), "inner").select(col("actual_barras").as("barras"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))

    // Se joinean los costos actuales y anteriores con los artículos con movimientos y sin cambio de precio
    val df_precios: DataFrame = df_join_precios.join(df_mov_agrupado, col("codigo_barra") === col("barras"), "inner").select(col("codigo_articulo").as("codigo"), col("barras"), col("fecha").as("fecha_stock"), col("movimientos_agrupados"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))

    // Se filtra el stock para el día de hoy (ya que al haber movimientos, se realizó el cálculo para tener el nuevo stock del día)
    val df_stock_filtrado = df_stock.filter(col("fecha_stock") === fechaHoy)

    // Se joinea el stock actual junto con los artículos con movimientos y sus precios, a su vez se calcula RxT
    val df_pre_final: DataFrame = df_stock_filtrado.as("stock").join(df_precios.as("precios"), Seq("codigo"), "inner").withColumn("resultado_por_tenencia",lit(0)).select(col("codigo"), col("barras"), col("precios.fecha_stock"), col("movimientos_agrupados").as("movimientos_agrupados"), col("stock.existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia")).distinct

    df_pre_final

  }

}