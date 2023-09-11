package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate


object sin_mov_ni_costo extends SparkSessionWrapper  {

  val fecha_stock_cero = "2022-06-30"
  val fecha_inicio_tiempos = "2000-01-02"
  def CalcularDataFrame(/*df_final: DataFrame, */df_movimientos: DataFrame, df_costo_unificado: DataFrame, df_articulos: DataFrame, df_stock: DataFrame, fecha_inicial: String, fecha_final: String): DataFrame = {

    val fecha_stock_cero = "2023-06-30"
    val fecha_inicio_tiempos = "2000-01-02"
    val fechaHoy = fecha_actual
    val fechaAyer = fecha_actual.minusDays(1)
    val df_movimientos_filtrado: DataFrame = df_movimientos.filter(col("fecha") === fechaHoy)
    val df_cu_dia: DataFrame = df_costo_unificado.filter(col("fecha_vigencia_desde") === fechaHoy)
    val no_mov_no_cu: DataFrame = df_articulos.select(col("barras")).except(df_movimientos_filtrado.select(col("codigo_barra"))).except(df_cu_dia.select(col("barras")))
    val df_sin_mov_sin_cu: DataFrame = df_articulos.join(no_mov_no_cu, Seq("barras"), "inner").select(col("barras"), col("codigo"))

    var new_df = new_df_param

    if (fechaAyer.toString == fecha_stock_cero) {

      // Filtrar los DataFrames para obtener solo los datos necesarios
      val df_stock_dia = df_stock.filter(col("fecha_stock") === fechaAyer)
      val df_cu_dia_menor = df_costo_unificado.filter(col("fecha_vigencia_desde") <= fechaAyer)
      val df_unico_precio = df_costo_unificado.filter(col("fecha_vigencia_desde") === fecha_inicio_tiempos)

      // Crear una copia de df_unico_precio con la fecha modificada
      val df_dup_unico_precio = df_unico_precio.withColumn("fecha_vigencia_desde", date_sub(col("fecha_vigencia_desde"), 1))

      // Unir los DataFrames y eliminar duplicados
      val df_union = df_cu_dia_menor.union(df_unico_precio).union(df_dup_unico_precio).distinct

      // Calcular el costo actual y anterior para cada código de barras
      val df_cu_dia_costo_actual = df_union.groupBy("barras").agg(max("fecha_vigencia_desde").as("fecha_mas_reciente"))
      val df_cu_dia_costo_anterior = df_union.join(df_cu_dia_costo_actual, Seq("barras"), "left").where(col("fecha_vigencia_desde") < col("fecha_mas_reciente")).groupBy("barras").agg(max("fecha_vigencia_desde").as("fecha_anterior"))

      // Obtener el precio actual y anterior para cada código de barras
      val df_precio_actual = df_cu_dia_costo_actual.as("actual").join(df_union.as("cu"), Seq("barras"), "inner").where(col("actual.fecha_mas_reciente") === col("cu.fecha_vigencia_desde")).select(col("barras").as("actual_barras"), col("costo").as("costo_unitario"), col("precio_minorista").as("precio_actual_minorista"), col("precio_mayorista").as("precio_actual_mayorista"))
      val df_precio_anterior = df_cu_dia_costo_anterior.as("anterior").join(df_union.as("cu"), Seq("barras"), "inner").where(col("anterior.fecha_anterior") === col("cu.fecha_vigencia_desde")).select(col("barras").as("anterior_barras"), col("costo").as("costo_anterior"))

      // Unir los precios actuales y anteriores
      val df_join_precios = df_precio_actual.join(df_precio_anterior, col("actual_barras") === col("anterior_barras"), "inner").select(col("actual_barras").as("barras"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))

      // Unir los precios con los artículos
      val df_precios = df_join_precios.join(df_articulos, Seq("barras"), "inner").select(col("codigo"), col("*"))

      // Joineo precios con stock del día
      val df_precio_stock = df_precios.as("precios").join(broadcast(df_stock_dia), Seq("codigo"), "inner").select(col("precios.*"), col("existencia").as("total_unidades"))

      // Cacheo el df intermedio
      df_precio_stock.cache()

      // Creo el df con todos los datos con fecha stock cero
      val df_stock_cero = df_precio_stock.withColumn("fecha_stock", lit(fecha_stock_cero)).withColumn("movimientos_agrupados", lit(0)).withColumn("resultado_por_tenencia", lit(0)).select(col("codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados"), col("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia"))

      // Traigo los artículos de df sin mov ni costo que no estén en df_final
      val df_art = df_sin_mov_sin_cu.select(col("barras")).except(df_final.select(col("barras")))

      // Joineo con artículos para traer todos los campos
      val df_articulos_filtrado = df_art.join(df_articulos, Seq("barras"), "inner")

      // Joineo precios con los artículos filtrados y me traigo costos y fechas
      val df_precios_hoy: DataFrame = df_join_precios.join(df_articulos_filtrado.as("art"), Seq("barras"), "inner").select(col("art.codigo").as("codigo"), col("barras"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))

      // Joineo stock con los precios del día
      val df_precio_stock_hoy = df_precios_hoy.as("precios").join(broadcast(df_stock_dia), Seq("codigo"), "inner").select(col("precios.*"), col("existencia").as("total_unidades"))

      // Cacheo el df intermedio
      df_precio_stock_hoy.cache()

      // Creo el df con todos los campos para el día de hoy
      val df_hoy = df_precio_stock_hoy.withColumn("fecha_stock", lit(fechaHoy)).withColumn("movimientos_agrupados", lit(0)).withColumn("resultado_por_tenencia", lit(0)).select(col("codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados"), col("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia"))

      new_df = df_stock_cero.union(df_hoy)

    } else {

      val df_filtrado: DataFrame = df_final.as("final").filter(col("fecha_stock") === fechaAyer).join(df_sin_mov_sin_cu, Seq("barras"), "inner").select(col("final.codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados"), col("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia"))

      val df_duplicado = df_filtrado.withColumn("fecha_stock", lit(fechaHoy)).withColumn("movimientos_agrupados", lit(0)).withColumn("resultado_por_tenencia", lit(0)).select(col("codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados"), col("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_minorista"), col("precio_actual_mayorista"), col("resultado_por_tenencia"))

      // Se filtra costo_unificado para traer todos los registros con las fechas anteriores a la de hoy
      val df_cu_dia_menor = df_costo_unificado.filter(col("fecha_vigencia_desde") <= fechaAyer)

      // Estos son los productos que no tienen cambios de precio, solamente tienen 1 registro histórico
      val df_unico_precio = df_costo_unificado.filter(col("fecha_vigencia_desde") === fecha_inicio_tiempos)

      // Se duplica el registro único y se le resta 1 día
      val df_dup_unico_precio = df_unico_precio.withColumn("fecha_vigencia_desde", date_sub(col("fecha_vigencia_desde"), 1))

      // Se unen el registro que se hardcodeó con el que se trae de costo_unificado
      val df_unico_precio_duplicado = df_unico_precio.union(df_dup_unico_precio)//.distinct

      // Unimos los códigos con la fecha hardcodeada con los artículos comunes
      val df_union = df_cu_dia_menor.union(df_unico_precio_duplicado).distinct

      // Se agrupa por artículo y se trae la fecha más reciente de costo_unificado (van a ser los precios que vamos a usar en el df a ingestar)
      val df_cu_dia_costo_actual: DataFrame = df_union.groupBy("barras").agg(max("fecha_vigencia_desde").as("fecha_mas_reciente"))

      // Se agrupa por artículo y se trae la 2da fecha más reciente de costo_unificado (va a ser nuestro costo_anterior)
      val df_cu_dia_costo_anterior: DataFrame = df_union.join(df_cu_dia_costo_actual, Seq("barras"), "left").where(col("fecha_vigencia_desde") < col("fecha_mas_reciente")).groupBy("barras").agg(max("fecha_vigencia_desde").as("fecha_anterior"))

      // Se joinea costo_unificado con la fecha más reciente para traer costo_unitario, precio_actual_mayorista y precio_actual_minorista
      val df_precio_actual: DataFrame = df_cu_dia_costo_actual.as("actual").join(df_union.as("cu"), Seq("barras"), "inner").where(col("actual.fecha_mas_reciente") === col("cu.fecha_vigencia_desde")).select(col("barras").as("actual_barras"), col("costo").as("costo_unitario"), col("precio_minorista").as("precio_actual_minorista"), col("precio_mayorista").as("precio_actual_mayorista"))

      // Se joinea costo_unificado con la 2da fecha más reciente para traer costo_anterior
      val df_precio_anterior: DataFrame = df_cu_dia_costo_anterior.as("anterior").join(df_union.as("cu"), Seq("barras"), "inner").where(col("anterior.fecha_anterior") === col("cu.fecha_vigencia_desde")).select(col("barras").as("anterior_barras"), col("costo").as("costo_anterior"))

      // Se joinean los df con los precios actuales y el df con costo_anterior
      val df_join_precios: DataFrame = df_precio_actual.join(df_precio_anterior, col("actual_barras") === col("anterior_barras"), "inner").select(col("actual_barras").as("barras"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))

      // Se joinean los costos actuales y anteriores con los artículos con movimientos y sin cambio de precio
      val df_precios: DataFrame = df_join_precios.join(df_sin_mov_sin_cu.as("art"), Seq("barras"), "inner").select(col("art.codigo").as("codigo"), col("barras"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))

      // Se filtra el stock para el día de hoy (ya que al haber movimientos, se realizó el cálculo para tener el nuevo stock del día)
      val df_stock_filtrado = df_stock.filter(col("fecha_stock") <= fechaHoy)

      // Se agrupa el stock del día y se trae la fecha más reciente (stock actual)
      val df_stock_agrupado = df_stock_filtrado.groupBy("codigo").agg(max("fecha_stock").as("max_fecha_stock")) //.orderBy(desc("max_fecha_stock"))

      // Joineamos la fecha más actual con el stock, para traer el valor de existencia junto con el código y la fecha_stock
      val df_join_stock = df_stock_agrupado.join(df_stock, Seq("codigo"), "inner").filter(col("fecha_stock") === col("max_fecha_stock")).select(col("codigo"), col("existencia"), col("fecha_stock"))

      val df_stock_precio = df_precios.as("precios").join(df_join_stock, Seq("codigo"), "inner").withColumn("fecha_stock", lit(fechaHoy)).select(col("precios.*"), col("existencia").as("total_unidades"), col("fecha_stock"))

      val df_pre_final = df_stock_precio.as("total").join(df_sin_mov_sin_cu, Seq("barras"), "inner").withColumn("movimientos_agrupados", lit(0)).withColumn("resultado_por_tenencia",lit(0)).select(col("total.codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados"), col("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia"))

      new_df = df_duplicado.union(df_pre_final)

    }
    new_df.distinct
  }
}

