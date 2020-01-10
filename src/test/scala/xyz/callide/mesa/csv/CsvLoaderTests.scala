package xyz.callide.mesa.csv


import org.scalatest.FlatSpec
import xyz.callide.mesa.data.conversion.ConversionSet

class CsvLoaderTests extends FlatSpec with CsvTest {

  behavior of "CsvLoader"

  it should "read from file" in {

    val data = CsvLoader.readFromFile(getClass.getClassLoader.getResource("data.csv").getFile)(ConversionSet())
    testCsvLoad(data)
  }

  it should "read from stream" in {

    val data = CsvLoader.readFromStream(getClass.getClassLoader.getResourceAsStream("data.csv"))(ConversionSet())
    testCsvLoad(data)
  }
}
