package xyz.callide.mesa.demo

import xyz.callide.mesa.data.DataSet

object Test {

  def main(args: Array[String]): Unit = {

    val data = DataSet.fromCsvResource("data5.csv")
    data.rows.foreach(row => println(row.elements))
  }
}
