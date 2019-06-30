package xyz.callide.mesa.data

trait DataSetUtil {

  protected val data3: DataSet = DataSet.fromCsvStream(getClass.getClassLoader.getResourceAsStream("data3.csv"))

  protected val data4: DataSet = DataSet.fromCsvStream(getClass.getClassLoader.getResourceAsStream("data4.csv"))

}
