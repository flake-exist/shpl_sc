import java.text.SimpleDateFormat

object SHPL_CONSTANTS {

  val shapley_anchor = (chain:String,ch_sep:String) => {
    val ch_list:Seq[String]   = chain.split(ch_sep) // create list of channels
    val freq:Map[String,Int]  = ch_list.groupBy(identity).mapValues(_.size) //frequency stat

    val unique_ch:Seq[String] = ch_list.distinct
    val cardinality:Int       = unique_ch.length

    val shpl_template:Seq[Double]   = List.fill(cardinality)(1.0/cardinality.toDouble)
    val shpl_val:Map[String,Double] = unique_ch.zip(shpl_template).toMap
    val result                      = ch_list.map(elem => shpl_val(elem) / freq(elem))
    result

  }

  val unix_to_date = (unix_seq_str:String,ch_sep:String,format_template:String) => {
    val unix_seq_long:Seq[Long] = unix_seq_str.split(ch_sep).map(_.toLong)
    val date_format:SimpleDateFormat = new SimpleDateFormat(format_template)
    val date_seq:Seq[String] = unix_seq_long.map(date_format.format(_))
    date_seq
  }
}
