import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

//args[0] - input folder
//args[1] - output folder
//args[2] - date format

import SHPL_CONSTANTS._
import CONSTANTS.{TRANSIT,CONCAT_SYMBOL}
import  SHPL_CONSTANTS._

object ShapleyCalc {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("Shapley Calc").getOrCreate()
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
      case "milisecond" => "yyyy-MM-dd:HH-mm-ss-ms"
      case e@_          => throw new Exception(s"`$e` format does not exist")

    }

    val data = spark.read.
      format("csv").
      option("inferSchema","false").
      option("header","true").
      option("mergeSchema","true").
      load(args(0))

//    data.show(20)

    val data_seq = data.
      withColumn("channels",split(col(USER_PATH),TRANSIT)).
      withColumn("shapley",shapley_anchor_udf($"channels")).
      withColumn("date_touch",unix_to_date_udf($"timeline",lit(TRANSIT),lit(format_template))).
      select($"channels",$"shapley",$"date_touch")

    data_seq.show(20)

    val data_conv = data_seq.
      withColumn("date_conv",element_at($"date_touch",-1))

    data_conv.show(20)
//
    val data_explode = data_conv.
      withColumn("touch_data",explode(arrays_zip($"shapley",$"date_touch",$"channels"))).
      select(
        $"touch_data.shapley".as("shapley"),
        $"touch_data.date_touch".as("date_touch"),
        $"touch_data.channels".as("channels"),
        $"date_conv")

    val data_shapley_touch = data_explode.
      groupBy($"channels",$"date_touch").
      agg(count($"shapley").as("shapley"))

    val data_shapley_conv = data_explode.
      groupBy($"channels",$"date_conv").
      agg(count($"shapley").as("shapley"))

    val data_shapley = data_explode.
      groupBy($"channels").
      agg(count($"shapley").as("shapley"))

    data_shapley.show(20)

//    val data_shapley = data_seq.
//      withColumn("shapley_value", shapley_anchor_udf(
//        col(USER_PATH),
//        lit(TRANSIT),
//        lit(format_template))).
//      select($"channels",$"shapley_value",$"date_touch")
//
//    val data_conv = data_shapley.
//      withColumn("date_conv", element_at($"date_touch", -1))
//
//    data_conv.show(20)

//    val data_explode = data_conv.
//      withColumn("touch_data",explode(arrays_zip($"shapley_value",$"date_touch",$"channels"))).
//      select(
//        $"touch_data.shapley_value".as("shapley_value"),
//        $"touch_data.date_touch".as("date_touch"),
//        $"touch_data.channels".as("channels"),
//        $"date_conv")
//
//
//    data_explode.
//      groupBy($"channels",$"date_touch",$"date_conv").
//      agg(count($"shapley_value").as("shapley_value"))
  }

}