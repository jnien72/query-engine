package com.test.qe

import com.test.qe.util.{HadoopUtils, SparkUtils}

object QueryEngine {
  def main(args:Array[String]):Unit={
    HadoopUtils.getHadoopConf()
    val sparkSession=SparkUtils.getSparkSession()
    val csisDf=sparkSession.read.parquet("dev/ecs/data/csis")
    csisDf.createOrReplaceTempView("csis")

    val csisDf2=sparkSession.sql(
      "select event_date, max(client.ip_address) " +
        "from csis " +
        "where event_date='2018-08-17'")

    csisDf2.show(1000)



//    sparkSession.sql("select * from csis")
  }
}
