package org.novakorp.com
import org.apache.spark.sql.functions._

object entry extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    val sucursal: String = args(0)
    val stock_table: String = args(1)
    val outputTable: String = args(2)
    val fecha_inicial_corrida: String = args(3)
    val fecha_final_corrida: String = args(4)

    print("FECHAS")
    println("")
    print(s"DESDE -> $fecha_inicial_corrida")
    println("")
    print(s"HASTA -> $fecha_final_corrida")
    println("")

    println("")
    println("----------------INICIANDO PROCESO PARA INSERTAR EN REF----------------")
    println("")
    println("Iniciando lectura de tablas...")

    val df_articulos = spark.sql("SELECT codigo,barras,id_seccion,id_departamento,id_grupo,id_rubro FROM raw.articulos WHERE enganchado = '0'")

    // Trae todos los movimientos
    val query_movimientos = s"""select codigo_barra,codigo_articulo,cantidad_movimiento,fecha,multiplicador_stock from cur.movimientos_detalle_unificado where fecha between '$fecha_inicial_corrida' and '$fecha_final_corrida' and sucursal='$sucursal'"""

    val df_movimientos = spark.sql(query_movimientos)

    val query_costo_unificado = """SELECT id_costo_unificado,fecha_vigencia_desde,barras,costo,precio_minorista,precio_mayorista FROM cur.costo_unificado """

    val df_costo_unificado = spark.sql(query_costo_unificado)

    val query_stock = s"select * FROM $stock_table"

    val df_stock = spark.sql(query_stock)

    val df_costo_unificado_con_anterior=functions.agregarCostoAnterior(df_costo_unificado)

    println("")
    println("Hecho!")
    println("")
    println("--> Iniciando procesamientos de registros... <--")
    println("")

    println("")

    // CON MOVIMIENTO Y CAMBIO DE PRECIO
    val df_con_mov_con_cu = con_mov_con_cu.CalcularDataFrame(df_movimientos, df_costo_unificado_con_anterior, df_stock,df_articulos, fecha_inicial_corrida, fecha_final_corrida)

    println("")
    println(s"Procesados con movimientos y cambio de precio para")
    println("")

    // CON MOVIMIENTOS SIN CAMBIO DE PRECIO
    val df_con_movimientos = con_movimientos.CalcularDataFrame(df_con_mov_con_cu,df_movimientos, df_costo_unificado_con_anterior, df_stock,df_articulos, fecha_inicial_corrida, fecha_final_corrida)

    println("")
    println(s"Procesados con movimientos ")
    println("")

    // CON CAMBIO DE PRECIO SIN MOVIMIENTO
    val df_cambio_precio = cambio_precio.CalcularDataFrame(df_movimientos, df_costo_unificado_con_anterior, df_stock,df_articulos, fecha_inicial_corrida, fecha_final_corrida)

    println("")
    println(s"Procesados con cambio precio")
    println("")

    // SIN CAMBIO Y SIN MOVIMIENTO
    val df_sin_mov_sin_cambio_precio = sin_mov_ni_costo.CalcularDataFrame(df_movimientos, df_costo_unificado_con_anterior, df_articulos, df_stock, fecha_inicial_corrida, fecha_final_corrida)

    println("")
    println(s"Procesados sin cambio precio ni movimientos ")
    println("")

    val df_hoy = df_con_mov_con_cu.union(df_con_movimientos).union(df_cambio_precio).union(df_sin_mov_sin_cambio_precio)

    val df_con_rxt=functions.calcularResultadoPorTenencia(df_hoy)

    val dfToInsert = df_con_rxt.withColumn("sucursal",lit(sucursal)).select(col("codigo"),col("barras"),col("movimientos_agrupados"),col("total_unidades"),col("costo_unitario"),col("costo_anterior"),col("precio_actual_mayorista"),col("precio_actual_minorista"),col("resultado_por_tenencia"),col("sucursal"),col("fecha_stock"))

    functions.saveCurrentDF(dfToInsert,outputTable)

    println(s"Terminada la ingesta en la sucursal $sucursal desde $fecha_inicial_corrida hasta $fecha_final_corrida")

  }
}
