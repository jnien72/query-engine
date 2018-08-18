package com.test.qe

import com.test.qe.util.HadoopUtils

object QueryEngine {
  def main(args:Array[String]):Unit={
    HadoopUtils.getHadoopConf()

    println("hi")

  }
}
