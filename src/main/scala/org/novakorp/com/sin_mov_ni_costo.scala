package org.novakorp.com
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object sin_mov_ni_costo extends SparkSessionWrapper  {

  def CalcularDataFrame(df_movimientos: DataFrame, df_costo_unificado_con_anterior: DataFrame, df_articulos: DataFrame, df_stock: DataFrame, fecha_inicial: String, fecha_final: String): DataFrame = {

      // Crear un DataFrame con todas las fechas en el período
      val df_fechas = spark.sql(s"SELECT sequence(to_date('$fecha_inicial'), to_date('$fecha_final'), interval 1 day) AS fecha").withColumn("fecha", explode(col("fecha")))

      val df_articulos_con_fechas = df_fechas.join(df_articulos.select(col("codigo").as("codigo_articulo"), col("barras").as("codigo_barra")), Seq.empty[String], "cross")

      val df_articulos_sin_mov = df_articulos_con_fechas.join(df_movimientos, Seq("codigo_articulo", "fecha"), "left_anti")

      val df_articulos_no_movimiento_no_costo =  df_articulos_sin_mov.join(df_costo_unificado_con_anterior.select(col("barras").as("codigo_barra"), col("fecha_vigencia_desde").as("fecha")), Seq("codigo_barra", "fecha"), "left_anti").withColumnRenamed("codigo_articulo","codigo")

      //Para cada codigo de barra con su fecha me traigo la fecha de su ultimo cambio de precio
      val joinedDF = df_articulos_no_movimiento_no_costo.join(df_costo_unificado_con_anterior,(df_articulos_no_movimiento_no_costo("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (df_costo_unificado_con_anterior("fecha_vigencia_desde") < df_articulos_no_movimiento_no_costo("fecha")),"inner").groupBy(df_articulos_no_movimiento_no_costo("codigo_barra"),df_articulos_no_movimiento_no_costo("fecha")).agg(max(df_costo_unificado_con_anterior("fecha_vigencia_desde")).alias("max_fecha_vigencia"))

      val df_costo_unificado_actual = joinedDF.join(df_costo_unificado_con_anterior.as("cu"),(joinedDF("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (joinedDF("max_fecha_vigencia") === df_costo_unificado_con_anterior("fecha_vigencia_desde")),"inner").select(col("cu.*"),joinedDF("fecha").as("fecha_df")).distinct()

      val df_precios = df_costo_unificado_actual.join(df_articulos_no_movimiento_no_costo, (df_costo_unificado_actual("barras") === df_articulos_no_movimiento_no_costo("codigo_barra")) && (df_costo_unificado_actual("fecha_df") === df_articulos_no_movimiento_no_costo("fecha")), "inner").select(col("codigo"), col("barras"), df_articulos_no_movimiento_no_costo("fecha").as("fecha_stock"), col("costo").as("costo_unitario"), col("costo_anterior"), col("precio_mayorista").as("precio_actual_mayorista"), col("precio_minorista").as("precio_actual_minorista"))

      val df_precios_con_max_stock = df_precios.join(df_stock,(df_precios("codigo") === df_stock("codigo")) && (df_stock("fecha_stock") < df_precios("fecha_stock")),"inner").groupBy(df_precios("codigo"),df_precios("fecha_stock")).agg(max(df_stock("fecha_stock")).alias("max_fecha_stock"))

      val df_sin_mov_sin_cu_max_stock = df_precios_con_max_stock.join(df_precios, Seq("codigo","fecha_stock"), "inner").select(df_precios("barras"),df_precios("fecha_stock"), col("costo_unitario"),col("costo_anterior"), col("precio_actual_minorista"), col("precio_actual_mayorista"),df_precios_con_max_stock("max_fecha_stock"),df_precios_con_max_stock("codigo"))

      val df_join_stock = df_sin_mov_sin_cu_max_stock.join(df_stock, df_sin_mov_sin_cu_max_stock("codigo")===df_stock("codigo")&&(df_sin_mov_sin_cu_max_stock("max_fecha_stock")===df_stock("fecha_stock")), "inner").select(df_sin_mov_sin_cu_max_stock("barras"),df_sin_mov_sin_cu_max_stock("costo_unitario"),df_sin_mov_sin_cu_max_stock("costo_anterior"),df_sin_mov_sin_cu_max_stock("precio_actual_minorista"),df_sin_mov_sin_cu_max_stock("precio_actual_mayorista"),df_stock("codigo"), df_stock("existencia"), df_sin_mov_sin_cu_max_stock("fecha_stock"))

      val df_pre_final = df_join_stock.withColumn("movimientos_agrupados", lit(0)).select(col("codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados"), col("existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"))

      df_pre_final

  }
}

