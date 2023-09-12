package org.novakorp.com
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import java.time.LocalDate
import java.time.format.DateTimeFormatter



object entry extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    val sucursal: String = args(0) //Ejemplo "CRESPO-VETE"
    val stock_table: String = args(1) //Ejemplo "raw_cr.vete_stock"
    val outputTable: String = args(2) //Ejemplo "refined.stock_diario_sin_desc"
    val fecha_inicial_corrida: String = args(3) //Va cambiando cada corrida, lo maneja NiFi Ej: 2023-07-10
    val fecha_final_corrida: String = args(4) //Va cambiando cada corrida, lo maneja NiFi Ejemplo 2023-07-20 (si fueran lotes de procesamiento de a 10 dias)

    // Obtén la fecha actual
    val fechaHoy = LocalDate.now()

    // Define el formato deseado
    val formato = DateTimeFormatter.ofPattern("yyyyMMdd")

    // Formatea la fecha en el formato 'yyyyMMdd' como un string
    val fechaFormateada = fechaHoy.format(formato)

    print("FECHAS")
    println("")
    print(s"DESDE -> ${fecha_inicial_corrida}")
    println("")
    print(s"HASTA -> ${fecha_final_corrida}")
    println("")

    println("")
    println("----------------INICIANDO PROCESO PARA INSERTAR EN REF----------------")
    println("")
    println("Iniciando lectura de tablas...")

    // Trae todos los códigos de barra y campos descriptivos de artículos UNICOS
    // TODO Aca podemos afinar QUE CAMPOS nos sirven en los procesamientos Que necesitamos del maestro de art?

    // Revisar que campos podemos EVITAR traernos al pedo del maestro de articulos y CUALES SE USAN PARA EL JOINEO DE CAMPOS DESCP
    val df_articulos = spark.sql("SELECT codigo,barras,id_seccion,id_departamento,id_grupo,id_rubro FROM raw.articulos WHERE enganchado = '0'").persist(StorageLevel.MEMORY_AND_DISK)

    // Trae todos los movimientos
    val query_movimientos = s"""select codigo_barra,codigo_articulo,cantidad_movimiento,fecha,multiplicador_stock from cur.movimientos_detalle_unificado where fecha between '${fecha_inicial_corrida}' and '${fecha_final_corrida}' and sucursal='${sucursal}'"""
    //val query_movimientos = "select codigo_barra,codigo_articulo,cantidad_movimiento,fecha,multiplicador_stock from cur.movimientos_detalle_unificado where fecha like '2022%' and sucursal='CRESPO-VETE'"

    val df_movimientos: DataFrame = spark.sql(query_movimientos).persist(StorageLevel.MEMORY_AND_DISK)

    // Trae todos los cambios de precio
    val query_costo_unificado = """SELECT id_costo_unificado,fecha_vigencia_desde,barras,costo,precio_minorista,precio_mayorista FROM cur.costo_unificado """

    val df_costo_unificado = spark.sql(query_costo_unificado).persist(StorageLevel.MEMORY_AND_DISK)

    // Trae todo Stock

    val query_stock = s"select * FROM ${stock_table}"

    val df_stock = spark.sql(query_stock).persist(StorageLevel.MEMORY_AND_DISK)

    println("")
    println("Hecho!")
    println("")
    println("--> Iniciando procesamientos de registros... <--")
    println("")

    println("")

    // CON MOVIMIENTO Y CAMBIO DE PRECIO
    val df_con_mov_con_cu = con_mov_con_cu.CalcularDataFrame(df_movimientos, df_costo_unificado, df_stock, fecha_inicial_corrida, fecha_final_corrida)

    println("")
    println(s"Procesados con movimientos y cambio de precio para")
    println("")

    // CON MOVIMIENTOS SIN CAMBIO DE PRECIO
    val df_con_movimientos = con_movimientos.CalcularDataFrame(df_con_mov_con_cu,df_movimientos, df_costo_unificado, df_stock, fecha_inicial_corrida, fecha_final_corrida)

    println("")
    println(s"Procesados con movimientos ")
    println("")

    // CON CAMBIO DE PRECIO SIN MOVIMIENTO
    val df_cambio_precio = cambio_precio.CalcularDataFrame(df_movimientos, df_costo_unificado, df_stock,df_articulos, fecha_inicial_corrida, fecha_final_corrida)

    println("")
    println(s"Procesados con cambio precio")
    println("")

    // SIN CAMBIO Y SIN MOVIMIENTO
    val df_sin_cambio_precio = sin_mov_ni_costo.CalcularDataFrame(df_movimientos, df_costo_unificado, df_articulos, df_stock, fecha_inicial_corrida, fecha_final_corrida)

    println("")
    println(s"Procesados sin cambio precio ni movimientos ")
    println("")

    val df_hoy = df_con_mov_con_cu.union(df_con_movimientos).union(df_cambio_precio).union(df_sin_cambio_precio)

    val dfToInsert: DataFrame = df_hoy.withColumn("sucursal",lit(sucursal.concat("_").concat(fechaFormateada)))
    functions.saveCurrentDF(dfToInsert,outputTable)

    println(s"Terminada la ingesta en la sucursal ${sucursal} desde ${fecha_inicial_corrida} hasta ${fecha_final_corrida}")

  }
}
