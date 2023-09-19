package org.novakorp.com
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object con_movimientos extends SparkSessionWrapper  {

  //CON MOVIMIENTOS

  def CalcularDataFrame(df_con_mov_con_cu:DataFrame, df_movimientos: DataFrame , df_costo_unificado_con_anterior: DataFrame, df_stock: DataFrame,fecha_inicial: String, fecha_final: String): DataFrame = {

    // Se filtran los artículos que no tienen cambio de precio en el día
    val df_con_mov_filtrado: DataFrame = df_movimientos.join(df_con_mov_con_cu, (df_movimientos("codigo_barra")===df_con_mov_con_cu("barras")) && (df_movimientos("fecha")===df_con_mov_con_cu("fecha_stock")), "left_anti")

    // Se agrupan los movimientos del día por codigo
    val df_mov_agrupado: DataFrame = df_con_mov_filtrado.groupBy("codigo_articulo", "fecha", "codigo_barra").agg(sum(col("cantidad_movimiento") * col("multiplicador_stock").cast("int")).as("movimientos_agrupados"))

    val joinedDF = df_mov_agrupado
      .join(df_costo_unificado_con_anterior,(df_mov_agrupado("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (df_costo_unificado_con_anterior("fecha_vigencia_desde") < df_mov_agrupado("fecha")),"inner")
      .groupBy(df_mov_agrupado("codigo_barra"),df_mov_agrupado("fecha")).agg(max(df_costo_unificado_con_anterior("fecha_vigencia_desde"))
      .alias("max_fecha_vigencia"))

    val df_costo_unificado_actual = joinedDF.join(df_costo_unificado_con_anterior.as("cu"),(joinedDF("codigo_barra") === df_costo_unificado_con_anterior("barras")) && (joinedDF("max_fecha_vigencia") === df_costo_unificado_con_anterior("fecha_vigencia_desde")),"inner").select(col("cu.*"),joinedDF("fecha").as("fecha_df")).distinct()

    val df_precios = df_costo_unificado_actual.join(df_mov_agrupado, (df_costo_unificado_actual("barras") === df_mov_agrupado("codigo_barra")) && (df_costo_unificado_actual("fecha_df") === df_mov_agrupado("fecha")), "inner").select(col("codigo_articulo").as("codigo"), col("barras"), df_mov_agrupado("fecha").as("fecha_stock"), col("movimientos_agrupados"), col("costo").as("costo_unitario"), col("costo_anterior"), col("precio_mayorista").as("precio_actual_mayorista"), col("precio_minorista").as("precio_actual_minorista"))

    // Se filtra el stock para el día de hoy (ya que al haber movimientos, se realizó el cálculo para tener el nuevo stock del día)
    val df_stock_filtrado = df_stock.filter(f"fecha_stock BETWEEN '$fecha_inicial' AND '$fecha_final'")

    // Se joinea el stock actual junto con los artículos con movimientos y sus precios, a su vez se calcula RxT
    val df_pre_final = df_stock_filtrado.as("stock").join(df_precios.as("precios"), Seq("codigo","fecha_stock"), "inner").select(col("codigo"), col("barras"), col("fecha_stock"), col("movimientos_agrupados").as("movimientos_agrupados"), col("stock.existencia").as("total_unidades"), col("costo_unitario"), col("costo_anterior"), col("precio_actual_mayorista"), col("precio_actual_minorista")).distinct

    df_pre_final

  }

}