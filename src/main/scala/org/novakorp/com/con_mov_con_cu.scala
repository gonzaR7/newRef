package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object con_mov_con_cu extends SparkSessionWrapper  {

    def CalcularDataFrame(df_movimientos: DataFrame , df_costo_unificado: DataFrame,  df_stock: DataFrame,fecha_inicial: String, fecha_final: String): DataFrame = {

        // Se filtran los artículos del día
        val df_con_mov: DataFrame = df_movimientos.filter(f"fecha BETWEEN ${fecha_inicial} AND ${fecha_final}")

        // Se agrupan los movimientos del día por codigo
        val df_mov_agrupado: DataFrame = df_con_mov.groupBy("codigo_articulo", "fecha", "codigo_barra").agg(sum(col("cantidad_movimiento") * col("multiplicador_stock").cast("int")).as("movimientos_agrupados"))

/*        // Se traen los cambios de precio del día
        val df_cu_dia_costo_actual: DataFrame = df_costo_unificado.filter(f"fecha_vigencia_desde BETWEEN ${fecha_inicial} AND ${fecha_final}").select(col("barras"), col("costo").as("costo_unitario"), col("precio_minorista"), col("precio_mayorista"), col("fecha_vigencia_desde").as("fecha_stock"))

        // Se joinean los artículos con movimientos y los con cambio de precio
        val df_con_mov_con_cu: DataFrame = df_cu_dia_costo_actual.as("actual").join(df_mov_agrupado, col("barras") === col("codigo_barra"), "inner").select(col("actual.*"), col("codigo_articulo").as("codigo"), col("movimientos_agrupados"))
*/
        // Definir una ventana ordenada por la columna "fecha_vigencia_desde" para cada "codigo_barras"
        val ventana = Window.partitionBy("barras").orderBy("fecha_vigencia_desde")

        // Costo unificado y en cada fila su costo anterior
        val df_costo_unificado_con_anterior = df_costo_unificado.withColumn("costo_anterior", lag("costo", 1).over(ventana)).filter(f"fecha_vigencia_desde BETWEEN ${fecha_inicial} AND ${fecha_final}")

        // Se joinean los artículos con movimientos y los con cambio de precio
        val df_con_mov_con_cu: DataFrame = df_costo_unificado_con_anterior.as("actual").join(df_mov_agrupado, col("barras") === col("codigo_barra"), "inner").select(col("actual.*"), col("codigo_articulo").as("codigo"), col("movimientos_agrupados"))

        // Se filtra el stock para el periodo correspondiente
        val df_stock_filtrado = df_stock.filter(f"fecha_stock BEETWEEN ${fecha_inicial} AND ${fecha_final}")

        // Se joinea el stock actual junto con los artículos con movimientos y sus precios, a su vez se calcula RxT
        val df_pre_final: DataFrame = df_stock_filtrado.as("stock").join(df_con_mov_con_cu.as("precios"), Seq("codigo"), "inner").withColumn("resultado_por_tenencia", ((col("costo_unitario") - col("costo_anterior")) * col("existencia"))).select(col("codigo"), col("barras"), col("precios.fecha_stock"), col("movimientos_agrupados"), col("stock.existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_mayorista").as("precio_actual_mayorista"), col("precio_minorista").as("precio_actual_minorista"), col("resultado_por_tenencia"))//.distinct

        df_pre_final
    }

}