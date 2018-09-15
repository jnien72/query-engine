package com.test.qe

import com.test.qe.util.{HadoopUtils, SparkUtils}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object TestRecommendations {
  def main(args:Array[String]):Unit={
    HadoopUtils.getHadoopConf()

    val sparkSession=SparkUtils.getSparkSession()

    val data = sparkSession.sparkContext.textFile("dev/rec/input/input.txt")

    val ratings = data.map(
      line => {
        val tokens= line.split(' ')
        val (user, item)=(tokens(0), tokens(1))
        Rating(user.toInt, item.toInt, 1.0)
      })

    val rank = 10
    val model = ALS.trainImplicit(ratings, rank, 5,0.001,20)

    import sparkSession.implicits._
    model.recommendProductsForUsers(5).toDF.show(false)

  }
}
