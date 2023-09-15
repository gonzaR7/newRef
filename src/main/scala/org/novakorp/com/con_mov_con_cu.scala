package org.novakorp.com
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object con_mov_con_cu extends SparkSessionWrapper  {

    def CalcularDataFrame(df_movimientos: DataFrame , df_costo_unificado_con_anterior: DataFrame,  df_stock: DataFrame,fecha_inicial: String, fecha_final: String): DataFrame = {

        // Se agrupan los movimientos del día por codigo
        val df_mov_agrupado = df_movimientos.groupBy("codigo_articulo", "fecha", "codigo_barra").agg(sum(col("cantidad_movimiento") * col("multiplicador_stock").cast("int")).as("movimientos_agrupados"))

        val df_costo_unificado_filtered= df_costo_unificado_con_anterior.filter(f"fecha_vigencia_desde BETWEEN $fecha_inicial AND $fecha_final")

        // Definir una ventana particionada por "fecha_vigencia_desde" y "barras"
        // y ordenada por "id_costo_unificado" en orden descendente
        val windowSpec = Window.partitionBy("fecha_vigencia_desde", "barras").orderBy(desc("id_costo_unificado"))

        // Agregar una columna de número de fila basada en la ventana
        val rankedDf = df_costo_unificado_filtered.withColumn("row_num", row_number().over(windowSpec))

        // Filtrar las filas donde row_num es igual a 1 (la primera fila en cada grupo)
        val resultDf = rankedDf.filter(col("row_num") === 1).drop("row_num")

        // Se joinean los artículos con movimientos y los con cambio de precio
        val df_con_mov_con_cu = resultDf.as("actual").join(df_mov_agrupado, (col("barras") === col("codigo_barra")) && (df_mov_agrupado("fecha") === col("fecha_vigencia_desde")), "inner").select(df_mov_agrupado("codigo_articulo").as("codigo"), resultDf("barras"), df_mov_agrupado("fecha").as("fecha_stock"), resultDf("costo").as("costo_unitario"), resultDf("costo_anterior"), resultDf("precio_mayorista").as("precio_actual_mayorista"), resultDf("precio_minorista").as("precio_actual_minorista"),df_mov_agrupado("movimientos_agrupados"))

        // Se filtra el stock para el periodo correspondiente
        val df_stock_filtrado = df_stock.filter(f"fecha_stock BETWEEN $fecha_inicial AND $fecha_final")

        // Se joinea el stock actual junto con los artículos con movimientos y sus precios, a su vez se calcula RxT
        val df_pre_final = df_stock_filtrado.as("stock").join(df_con_mov_con_cu.as("precios"),(df_stock_filtrado("codigo")===df_con_mov_con_cu("codigo"))&&(df_stock_filtrado("fecha_stock")===df_con_mov_con_cu("fecha_stock")) , "inner").withColumn("resultado_por_tenencia", (col("costo_unitario") - col("costo_anterior")) * col("existencia")).select(df_con_mov_con_cu("codigo"), col("barras"), col("precios.fecha_stock"), col("movimientos_agrupados"), col("stock.existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista"), col("resultado_por_tenencia")).distinct

        df_pre_final
    }
}