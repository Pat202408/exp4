import org.apache.spark.{SparkConf, SparkContext}

object CouponUsageCountRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("CouponUsageCountRDD")
      .setMaster("local[*]") 

    val sc = new SparkContext(conf)

    val filePath = "hdfs://localhost:9000/user/lcx/input/ccf_online_stage1_train.csv"
    val rawRDD = sc.textFile(filePath)

    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(line => line != header)

    val couponUsedRDD = dataRDD
      .map(line => line.split(","))
      .filter(fields =>
        fields.length >= 7 &&
        fields(3) != "null" && fields(3).nonEmpty &&   
        fields(6) != "null" && fields(6).nonEmpty  
      )
      .map(fields => (fields(3), 1)) 

    val couponCountRDD = couponUsedRDD
      .reduceByKey(_ + _)

    val sortedRDD = couponCountRDD
      .map { case (couponId, cnt) => (cnt, couponId) }
      .sortByKey(ascending = false)
      .map { case (cnt, couponId) => (couponId, cnt) }

    sortedRDD
      .map { case (couponId, cnt) => s"$couponId $cnt" }
      .saveAsTextFile("hdfs://localhost:9000/user/lcx/output/coupon_usage_count")

    println("Top 10 Coupons by Usage Count:")
    sortedRDD.take(10).foreach {
      case (couponId, cnt) =>
        println(s"$couponId $cnt")
    }

    sc.stop()
  }
}

