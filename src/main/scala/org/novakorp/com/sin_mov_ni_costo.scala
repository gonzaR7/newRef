package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate


object sin_mov_ni_costo extends SparkSessionWrapper  {

  def CalcularDataFrame(df_movimientos: DataFrame, df_costo_unificado: DataFrame, df_articulos: DataFrame, df_stock: DataFrame, fecha_inicial: String, fecha_final: String): DataFrame = {

      // Definir una ventana ordenada por la columna "fecha_vigencia_desde" para cada "codigo_barras"
      val ventana = Window.partitionBy("barras").orderBy("id_costo_unificado")

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
        .join(df_costo_unificado_con_anterior,(df_articulos_no_movimiento_no_costo("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (df_costo_unificado_con_anterior("fecha_vigencia_desde") < df_articulos_no_movimiento_no_costo("fecha")),"inner")
        .groupBy(df_articulos_no_movimiento_no_costo("codigo_barra"),df_articulos_no_movimiento_no_costo("fecha")).agg(max(df_costo_unificado_con_anterior("fecha_vigencia_desde"))
        .alias("max_fecha_vigencia"))

    //PARA MI DEBERIA SER ASI: LE AGREGO LA FECHA EN df_costo_unificado_actual DEL df_articulos_no_movimiento_no_costo Y LUEGO EN df_precios JOINEO POR BARRAS Y FECHA
      val df_costo_unificado_actual = joinedDF.join(df_costo_unificado_con_anterior.as("cu"),(joinedDF("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (joinedDF("max_fecha_vigencia") === df_costo_unificado_con_anterior("fecha_vigencia_desde")),"inner").select(col("cu.*"),joinedDF("fecha").as("fecha_df")).distinct()

      val df_precios = df_costo_unificado_actual.join(df_articulos_no_movimiento_no_costo, (df_costo_unificado_actual("barras") === df_articulos_no_movimiento_no_costo("codigo_barra")) && (df_costo_unificado_actual("fecha_df") === df_articulos_no_movimiento_no_costo("fecha")), "inner").select(col("codigo"), col("barras"), df_articulos_no_movimiento_no_costo("fecha").as("fecha_stock"), col("costo").as("costo_unitario"), col("costo_anterior"), col("precio_mayorista").as("precio_actual_mayorista"), col("precio_minorista").as("precio_actual_minorista"))

        // Se filtra el stock para el día de hoy (ya que al haber movimientos, se realizó el cálculo para tener el nuevo stock del día)
      val df_stock_filtrado = df_stock.filter(f"fecha_stock BETWEEN ${fecha_inicial} AND ${fecha_final}")

        // Se joinea el stock actual junto con los artículos con movimientos y sus precios, a su vez se calcula RxT
      val df_pre_final = df_stock_filtrado.as("stock").join(df_precios.as("precios"), Seq("codigo","fecha_stock"), "inner").withColumn("resultado_por_tenencia",lit(0)).select(col("codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados").as("movimientos_agrupados"), col("stock.existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia")).distinct

      df_articulos_no_movimiento_no_costo

  }
}

