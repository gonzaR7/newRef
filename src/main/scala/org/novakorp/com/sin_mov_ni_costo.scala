package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate


object sin_mov_ni_costo extends SparkSessionWrapper  {

  def CalcularDataFrame(df_movimientos: DataFrame, df_costo_unificado: DataFrame, df_articulos: DataFrame, df_stock: DataFrame, fecha_inicial: String, fecha_final: String): DataFrame = {

      // Definir una ventana ordenada por la columna "fecha_vigencia_desde" para cada "codigo_barras"
      val ventana = Window.partitionBy("barras").orderBy("fecha_vigencia_desde")

      // Costo unificado y en cada fila su costo anterior
      val df_costo_unificado_con_anterior = df_costo_unificado.withColumn("costo_anterior", when(lag("costo", 1).over(ventana).isNull, col("costo")).otherwise(lag("costo", 1).over(ventana)))

      // Crear un DataFrame con todas las fechas en el período
      val df_fechas = spark.sql(s"SELECT sequence(to_date('$fecha_inicial'), to_date('$fecha_final'), interval 1 day) AS fecha").withColumn("fecha", explode(col("fecha")))

      // Obtengo los articulos que no tuvieron ni movimiento ni cambio de precio para cada dia del Periodo
      val df_articulos_no_movimiento_no_costo = df_fechas.join(df_articulos.select(col("codigo"), col("barras").as("codigo_barra")), Seq.empty[String], "cross")
        .join(df_movimientos, Seq("codigo_barra", "fecha"), "left_anti")
        .join(df_costo_unificado.select(col("barras").as("codigo_barra"), col("fecha_vigencia_desde").as("fecha")), Seq("codigo_barra", "fecha"), "left_anti")

        //Para cada codigo de barra con su fecha me traigo la fecha de su ultimo cambio de precio
      val joinedDF = df_articulos_no_movimiento_no_costo
        .join(df_costo_unificado_con_anterior,(df_articulos_no_movimiento_no_costo("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (df_costo_unificado_con_anterior("fecha_vigencia_desde") < df_mov_agrupado("fecha")),"inner")
        .groupBy(df_articulos_no_movimiento_no_costo("codigo_barra"),df_articulos_no_movimiento_no_costo("fecha")).agg(max(df_costo_unificado_con_anterior("fecha_vigencia_desde"))
        .alias("max_fecha_vigencia"))

        //Para tener los campos de df_costo_unificado_con_anterior
      val df_costo_unificado_actual = joinedDF.join(df_costo_unificado_con_anterior.as("cu"),(joinedDF("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (joinedDF("max_fecha_vigencia") === df_costo_unificado_con_anterior("fecha_vigencia_desde")),"inner").select(col("cu.*"))

      df_articulos_no_movimiento_no_costo

  }
}

