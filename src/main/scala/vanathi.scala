import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import java.util.Properties



object vanathi extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = SparkSession.builder()
    .appName("Data_Analysis")
    .getOrCreate()

  val patients_schema="subject_id Int, gender String, anchor_age Int, anchor_year String, anchor_year_group String, dod String"


  var patients_df = spark.read.option("header", "true")
    .schema(patients_schema)
    .csv(args(0))
  val patients_df_new = patients_df.select("anchor_year")

  patients_df_new.write.option("header", "true").csv(args(1))

}

