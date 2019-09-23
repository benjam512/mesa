# mesa

Simple library for the loading and manipulation of tabular data in Scala

## Building from Source

The library can be built with SBT >= 1.2.8

## Working with Data Sets

The main abstraction is the `DataSet` class, which is an immutable container for a set of tabular data. Data can be loaded either
from a CSV file or from a JDBC `ResultSet`. Once instantiated, we can perform various SQL-like operations on a `DataSet` instance,
such as filtering and grouping. Note that `DataSet`s are immutable objects, and thus all operations result in a new instance.

## Data Forms

The following Scala types are currently supported

  * Boolean
  * LocalDate
  * LocalDateTime
  * Double
  * Int
  * Long
  * String

In the case of loading data from a CSV file, the types are inferred automatically. The general strategy is to use the most compact
representation of the data as possible. Any record that is not able to be represented as a more specific type is set to its string
representation as a fallback. In the case of loading from a `ResultSet`, the data type is determined explicitly from the `ResultSet`
metadata. Types that are not conformable with the list above are represented as strings.

## Conversion Sets

In order to store data in the appropriate type, we need to implement a `Converter` for that particular type. The `Converter` is
responsible for taking in an arbitrary input of type `Any`, and attempting to convert it to the specified result type. Default
converters are provided for all of the supported data types, which provide "reasonable" behavior for most use cases.

To load data from either a CSV for a `ResultSet`, we need an implicit instance of a `ConversionSet`, which is simply a complete
set of individual `Converter`s that covers all supported data types. A default `ConversionSet` is provided which is comprised of
each data type's default `Converter`.

## Examples

### Loading Data

```scala
// use the default conversion set
val dataFromCsv = DataSet.fromCsvFile(path = "/home/user/mydata.csv", delimiter = ',', header = true)

// use a custom conversion set
val dataFromCsv = DataSet.fromCsvFile(path = "/home/user/mydata.csv")(MyCustomConversionSet())

// get data from a database
val results: ResultSet = query(...)
val dataFromSql = DataSet.fromResultSet(results)
```

### Manipulating Data

```scala
// filter on single field
data.filter("age")(point => point.as[Int] > 25)

// filter on multiple fields
data.filter(row => row[Int]("age") > 25 && row[Double]("score") < 84.5)

// creates a Map[Int, DataSet] connecting each unique age to its subset of data
data.group("age")(_.as[Int])

// count rows that match condition
data.count("score")(point => point.as[Double] < 65.0 || point.as[Double] > 95.0)

```