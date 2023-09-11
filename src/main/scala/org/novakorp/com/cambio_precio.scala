package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate


object cambio_precio extends SparkSessionWrapper  {

  val fecha_inicio_tiempos = "2000-01-02"
  def CalcularDataFrame(df_movimientos: DataFrame , df_costo_unificado: DataFrame , df_articulos: DataFrame , df_stock: DataFrame , fecha_inicial: String, fecha_final: String) : DataFrame = {

    val fechaHoy = fecha_actual
    //val fechaAyer = fecha_actual.minusDays(1)
    val fecha_inicio_tiempos = "2000-01-02"

    // La diferencia entre los registros de df_con_mov_sin_cu vs df_pre_final se debe a que hay artículos que tienen más de un cambio de precio por día, en ese caso se toma el más actual

    // Se filtran los artículos del día
    val df_con_mov: DataFrame = df_movimientos.filter(col("fecha") === fechaHoy)
    // Se traen los artículos sin movimientos del día
    val df_sin_mov: DataFrame = df_articulos.as("art").select(col("barras")).except(df_con_mov.select(col("codigo_barra"))).select(col("art.barras"))
    // Se traen los artículos con cambio de precio
    val df_con_cu: DataFrame = df_costo_unificado.filter(col("fecha_vigencia_desde") === fechaHoy)
    // Se joinea y se traen los artículos con cambio de precio y sin movimiento
    val df_sin_mov_cu: DataFrame = df_con_cu.as("cu").join(df_sin_mov, col("cu.barras") === col("art.barras"), "inner").select(col("cu.*")).join(df_articulos, Seq("barras"), "inner").select(col("cu.*"), col("codigo"))
    // Se filtra costo_unificado para traer todos los registros con las fechas anteriores a la de hoy
    val df_cu_dia_menor: DataFrame = df_costo_unificado.filter(col("fecha_vigencia_desde") <= fechaHoy)
    // Estos son los productos que no tienen cambios de precio, solamente tienen 1 registro histórico
    val df_unico_precio = df_costo_unificado.filter(col("fecha_vigencia_desde") === fecha_inicio_tiempos )
    // Se duplica el registro único y se le resta 1 día
    val df_dup_unico_precio = df_unico_precio.withColumn("fecha_vigencia_desde", date_sub(col("fecha_vigencia_desde"), 1))
    // Se unen el registro que se hardcodeó con el que se trae de costo_unificado
    val df_unico_precio_duplicado = df_unico_precio.union(df_dup_unico_precio).distinct
    // Unimos los códigos con la fecha hardcodeada con los artículos comunes
    val df_union = df_cu_dia_menor.union(df_unico_precio_duplicado).distinct
    // Se agrupa por artículo y se trae la fecha más reciente de costo_unificado (van a ser los precios que vamos a usar en el df a ingestar)
    val df_cu_dia_costo_actual: DataFrame = df_union.groupBy("barras").agg(max("fecha_vigencia_desde").as("fecha_mas_reciente"))
    // Se agrupa por artículo y se trae la 2da fecha más reciente de costo_unificado (va a ser nuestro costo_anterior)
    val df_cu_dia_costo_anterior: DataFrame = df_union.join(df_cu_dia_costo_actual, Seq("barras"), "left").where(col("fecha_vigencia_desde") < col("fecha_mas_reciente")).groupBy("barras").agg(max("fecha_vigencia_desde").as("fecha_anterior"))
    // Se joinea costo_unificado con la fecha más reciente para traer costo_unitario, precio_actual_mayorista y precio_actual_minorista
    val df_precio_actual: DataFrame = df_cu_dia_costo_actual.as("actual").join(df_union.as("cu"), Seq("barras"), "inner").where(col("actual.fecha_mas_reciente") === col("cu.fecha_vigencia_desde")).select(col("barras").as("actual_barras"), col("fecha_vigencia_desde").as("fecha_stock"), col("costo").as("costo_unitario"), col("precio_minorista").as("precio_actual_minorista"), col("precio_mayorista").as("precio_actual_mayorista"))
    // Se joinea costo_unificado con la 2da fecha más reciente para traer costo_anterior
    val df_precio_anterior: DataFrame = df_cu_dia_costo_anterior.as("anterior").join(df_union.as("cu"), Seq("barras"), "inner").where(col("anterior.fecha_anterior") === col("cu.fecha_vigencia_desde")).select(col("barras").as("anterior_barras"), col("costo").as("costo_anterior"))
    // Se joinean los df con los precios actuales y el df con costo_anterior
    val df_join_precios: DataFrame = df_precio_actual.join(df_precio_anterior, col("actual_barras") === col("anterior_barras"), "inner").select(col("actual_barras").as("barras"), col("fecha_stock"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))
    // Se joinean los costos actuales y anteriores con los artículos sin movimientos y con cambio de precio
    val df_precios: DataFrame = df_join_precios.join(df_sin_mov_cu, Seq("barras"), "inner").select(col("codigo"), col("barras"), col("fecha_stock"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))
    // Se filtra el stock para el día de hoy (ya que al haber movimientos, se realizó el cálculo para tener el nuevo stock del día)
    val df_stock_filtrado = df_stock.filter(col("fecha_stock") <= fechaHoy)
    // Se agrupa el stock del día y se trae la fecha más reciente (stock actual)
    val df_stock_agrupado = df_stock_filtrado.groupBy("codigo").agg(max("fecha_stock").as("max_fecha_stock"))//.orderBy(desc("max_fecha_stock"))
    // Joineamos la fecha más actual con el stock, para traer el valor de existencia junto con el código y la fecha_stock
    val df_join_stock = df_stock_agrupado.join(df_stock, Seq("codigo"), "inner").filter(col("fecha_stock") === col("max_fecha_stock")).select(col("codigo"), col("existencia"), col("fecha_stock"))
    // Se joinea el stock actual junto con los artículos con movimientos y sus precios, a su vez se calcula RxT
    val df_pre_final: DataFrame = df_join_stock.as("stock").join(df_precios.as("precios"), Seq("codigo"), "inner").withColumn("movimientos_agrupados", lit(0)).withColumn("resultado_por_tenencia", ((col("costo_unitario") - col("costo_anterior")) * col("existencia"))).select(col("codigo"), col("barras"), col("precios.fecha_stock"), col("movimientos_agrupados"), col("stock.existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia")).distinct

    df_pre_final
  }
}