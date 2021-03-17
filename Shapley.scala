import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

//args[0] - input folder
//args[1] - output folder
//args[2] - date format

import SHPL_CONSTANTS._
//import CONSTANTS.{TRANSIT,CONCAT_SYMBOL}

object Shapley {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("Shapley").getOrCreate()
    import spark.implicits._

    val shapley_anchor_udf = spark.udf.register("shapley_anchor_udf",shapley_anchor)
    val unix_to_date_udf   = spark.udf.register("unix_to_date_udf",unix_to_date)

    val format_template = args(2).toLowerCase match {
      case "year"       => "yyyy"
      case "month"      => "yyyy-MM"
      case "day"        => "yyyy-MM-dd"
      case "hour"       => "yyyy-MM-dd:HH"
      case "minute"     => "yyyy-MM-dd:HH-mm"
      case "second"     => "yyyy-MM-dd:HH-mm-ss"
      case "milisecond" => "yyyy-MM-dd:HH-mm-ss=ms"
      case e@_          => throw new Exception(s"`$e` format does not exist")

    }

    val data = spark.read.
      format("csv").
      option("inferSchema","false").
      option("header","true").
      option("mergeSchema","true").
      load(args(0))

    data.show(20)

    val data_seq = data.
      withColumn("channels",split($"user_path","=>")).
      withColumn("date_touch",split($"timeline","=>")).
      withColumn("shapley_value", shapley_anchor_udf(
        col("user_path"),
        lit("=>"),
        lit(format_template))).
      select($"channels",$"shapley_value",$"date_touch")

    val data_conv = data_seq.
      withColumn("date_conv", element_at($"date_touch", -1))

    val data_explode = data_conv.
      withColumn("touch_data",explode(arrays_zip($"shapley_value",$"date_touch",$"channels"))).
      select(
        $"touch_data.shapley_value".as("shapley_value"),
        $"touch_data.date_touch".as("date_touch"),
        $"touch_data.channels".as("channels"),
        $"date_conv")


    data_explode.
      groupBy($"channels",$"date_touch",$"date_conv").
      agg(count($"shapley_value").as("shapley_value"))
  }

}
