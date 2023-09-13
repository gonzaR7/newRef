import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("Simple Application")
  .master("local[*]")
  .getOrCreate()


val path: String= "E:/Escritorio/bsf_330_contactc_contactcenter_54_20220401.dat"
val df = spark
  .read
  .option("delimiter","|")
  .option("header","true")
  .option("inferSchema","true")
  .csv(path)

df.show()