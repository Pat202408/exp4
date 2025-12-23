import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}

object O2OCouponPredict {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("O2O Coupon Predict")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val offline = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("input/ccf_offline_stage1_train.csv")

    val train = offline
      .filter($"Coupon_id" =!= "null" && $"Date_received" =!= "null")
      .withColumn("date_received_dt", to_date($"Date_received", "yyyyMMdd"))
      .withColumn(
          "date_used_dt",
          when($"Date" === "null", null)
           .otherwise(to_date($"Date", "yyyyMMdd"))
        )
      .withColumn(
        "diff_days",
        datediff($"date_used_dt", $"date_received_dt")
      )
      .withColumn(
        "label",
        when(
          $"date_used_dt".isNotNull &&
          $"diff_days" >= 0 &&
          $"diff_days" <= 15,
          1.0
        ).otherwise(0.0)
      )
      
    
    val discountUDF = udf { s: String =>
      if (s == "null" || s.isEmpty) {
        1.0
      } else if (s.contains(":")) {
        val parts = s.split(":")
        val x = parts(0).toDouble
        val y = parts(1).toDouble
        (x - y) / x
      } else {
        s.toDouble
      }
    }
    
    val featureDF = train
      .withColumn("discount_rate_num", discountUDF($"Discount_rate"))
      .withColumn("is_manjian", when($"Discount_rate".contains(":"), 1).otherwise(0))
      .withColumn(
        "distance",
        when($"Distance" === "null", 10)
          .otherwise($"Distance".cast("int"))
      )
      .withColumn("received_day", dayofmonth($"date_received_dt"))
      .withColumn("received_week", weekofyear($"date_received_dt"))

    val userIndexer = new StringIndexer()
      .setInputCol("User_id")
      .setOutputCol("user_idx")
      .setHandleInvalid("keep")

    val merchantIndexer = new StringIndexer()
      .setInputCol("Merchant_id")
      .setOutputCol("merchant_idx")
      .setHandleInvalid("keep")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "discount_rate_num",
        "is_manjian",
        "distance",
        "received_day",
        "received_week",
        "user_idx",
        "merchant_idx"
      ))
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(50)
      .setRegParam(0.01)
      .setElasticNetParam(0.0)

    val pipeline = new Pipeline()
      .setStages(Array(
        userIndexer,
        merchantIndexer,
        assembler,
        lr
      ))

    val model = pipeline.fit(featureDF)
    val testRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("input/ccf_offline_stage1_test_revised.csv")

    val testFeature = testRaw
      .withColumn("discount_rate_num", discountUDF($"Discount_rate"))
      .withColumn("is_manjian", when($"Discount_rate".contains(":"), 1).otherwise(0))
      .withColumn(
        "distance",
        when($"Distance" === "null", 10)
          .otherwise($"Distance".cast("int"))
      )
      .withColumn(
        "date_received_dt",
        to_date($"Date_received", "yyyyMMdd")
      )
      .withColumn("received_day", dayofmonth($"date_received_dt"))
      .withColumn("received_week", weekofyear($"date_received_dt"))

    val predictDF = model.transform(testFeature)
    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.functions.udf
    
    val vectorToArray = udf((v: Vector) => v.toArray)
    val result = predictDF
      .withColumn("prob_array", vectorToArray($"probability"))
      .withColumn("Probability", col("prob_array").getItem(1))
      .select(
        $"User_id",
        $"Merchant_id",
        $"Coupon_id",
        $"Discount_rate",
        $"Distance",
        $"Date_received",
        $"Probability"
      )
    result
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/submission.csv")

    spark.stop()
  }
}

