package org.novakorp.com
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object cambio_precio extends SparkSessionWrapper  {

  def CalcularDataFrame(df_movimientos: DataFrame , df_costo_unificado_con_anterior: DataFrame , df_stock: DataFrame, df_articulos: DataFrame, fecha_inicial: String, fecha_final: String) : DataFrame = {

    // Definir una ventana particionada por "fecha_vigencia_desde" y "barras"
    // y ordenada por "id_costo_unificado" en orden descendente
    val windowSpec = Window.partitionBy("fecha_vigencia_desde", "barras").orderBy(desc("id_costo_unificado"))

    // Agregar una columna de número de fila basada en la ventana
    val rankedDf = df_costo_unificado_con_anterior.withColumn("row_num", row_number().over(windowSpec))

    // Filtrar las filas donde row_num es igual a 1 (la primera fila en cada grupo)
    val resultDf = rankedDf.filter(col("row_num") === 1).drop("row_num")

    val df_costo_unificado_periodo=resultDf.filter(f"fecha_vigencia_desde BETWEEN '$fecha_inicial' AND '$fecha_final'")

    val df_enganchado = df_movimientos.join(df_articulos,df_movimientos("codigo_articulo") === df_articulos("codigo"),"inner").select(df_movimientos("*"),df_articulos("barras").as("barras_enganchado"))

    val df_sin_mov_cu_temp=df_costo_unificado_periodo.as("cu").join(df_enganchado, (df_costo_unificado_periodo("barras")===df_enganchado("barras_enganchado"))  && (df_costo_unificado_periodo("fecha_vigencia_desde") === df_enganchado("fecha")),"left_anti").select(col("cu.*")).withColumnRenamed("fecha_vigencia_desde","fecha_stock")

    // Join con articulos para agregar Codigo.
    val df_sin_mov_cu=df_sin_mov_cu_temp.as("cu").join(df_articulos, df_sin_mov_cu_temp("barras")===df_articulos("barras"),"inner").select(col("cu.*"),col("codigo"))

    val joinedDF = df_sin_mov_cu
      .join(df_stock,(df_sin_mov_cu("codigo") === df_stock("codigo")) && (df_stock("fecha_stock") < df_sin_mov_cu("fecha_stock")),"inner")
      .groupBy(df_sin_mov_cu("codigo"),df_sin_mov_cu("fecha_stock")).agg(max(df_stock("fecha_stock"))
      .alias("max_fecha_stock"))

    // Joineamos la fecha más actual con el stock, para traer el valor de existencia junto con el código y la fecha_stock

    val df_sin_mov_cu_max_stock = joinedDF.join(df_sin_mov_cu, Seq("codigo","fecha_stock"), "inner").select(df_sin_mov_cu("barras"),df_sin_mov_cu("fecha_stock"), df_sin_mov_cu("costo").as("costo_unitario"),df_sin_mov_cu("costo_anterior"), df_sin_mov_cu("precio_minorista").as("precio_actual_minorista"), df_sin_mov_cu("precio_mayorista").as("precio_actual_mayorista"),joinedDF("max_fecha_stock"),joinedDF("codigo"))

    val df_join_stock = df_sin_mov_cu_max_stock.join(df_stock, df_sin_mov_cu_max_stock("codigo")===df_stock("codigo")&&(df_sin_mov_cu_max_stock("max_fecha_stock")===df_stock("fecha_stock")), "inner").select(df_sin_mov_cu_max_stock("barras"),df_sin_mov_cu_max_stock("costo_unitario"),df_sin_mov_cu_max_stock("costo_anterior"),df_sin_mov_cu_max_stock("precio_actual_minorista"),df_sin_mov_cu_max_stock("precio_actual_mayorista"),df_stock("codigo"), df_stock("existencia"), df_sin_mov_cu_max_stock("fecha_stock"))

    val df_pre_final=  df_join_stock.withColumn("movimientos_agrupados", lit(0)).select(col("codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados"), col("existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista")).distinct

    df_pre_final
  }
}