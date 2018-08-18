package com.test.qe.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

  private var sparkSession:SparkSession=null

  def getSparkSession():SparkSession={
    this.synchronized{
      if(sparkSession==null){
        val conf = new SparkConf()
        EnvProperty.getEntriesWithPrefix("spark").foreach(entry=>{
          conf.set(entry._1,entry._2)
        })
        EnvProperty.getEntriesWithPrefix("javax").foreach(entry=>{
          conf.set(entry._1,entry._2)
        })
        sparkSession=SparkSession.builder().config(conf).getOrCreate()
      }
    }
    sparkSession
  }

  def getSparkContext():SparkContext={
    getSparkSession().sparkContext
  }

}

