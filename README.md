# mesa

Simple library for the loading and manipulation of tabular data in Scala

## Building from Source

The library can be built with SBT >= 1.2.8

## Working with Data Sets

The main abstraction is the `DataSet` class, which is an immutable container for a set of tabular data. Data can be loaded either
from a CSV file or from a JDBC `ResultSet`. Once instantiated, we can perform various SQL-like operations on a `DataSet` instance,
such as filtering and grouping. Note that `DataSet`s are immutable objects, and thus all transformations result in a new instance
being created.

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

To load data from either a CSV or a `ResultSet`, we need an implicit instance of a `ConversionSet`, which is simply a complete
set of individual `Converter`s that covers all supported data types. A default `ConversionSet` is provided which is comprised of
each data type's default `Converter`.

## Missing Values

If loading from a CSV file, any empty or blank-space entries are interpreted as missing values. Likewise, when loading from 
a result set, any null values are considered to be missing data points. Internally, each field is stored as an array of 
optional values, and we can use this to elegantly handle cases of missing data.

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

### Handling Missing Data

```scala
val field = data("score") // [83.5, 75.1, 94.2, ?, 87.6]
field.extract[Double] // simply filter out missing values => [83.5, 75.1, 94.2, 87.6]
field.extract[Double](0.0) // provide default value => [83.5, 75.1, 94.2, 0.0, 87.6]

field.point(0).as[Double] // 83.5
field.point(0).get[Double] // Some(83.5)

field.point(3).as[Double] // runtime error!
field.point(3).get[Double] // None
```

### Custom Conversions

Let's say that we want to implement a custom date converter like this:

```scala
object CustomLocalDateConverter extends Converter[LocalDate] {
  private val formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")
  override def convert(value: Any): LocalDate = value match {
    case v: LocalDate => v
    case v: LocalDateTime => v.toLocalDate
    case v: String => LocalDate.parse(v, formatter)
    case v => throw InvalidConversionException[LocalDate](v)
  }
}
```

We have a couple of options for using this converter. One method would be to create a new conversion set which uses
this custom converter at the time of loading the data.

```scala
implicit val conversion: ConversionSet = ConversionSet().copy(localDateConverter = CustomLocalDateConverter)
val data = DataSet.fromCsvFile("/tmp/data.csv")
```

The other option is to load the data without modification, and use the converter when extracting that field.

```scala
implicit val dateConverter: Converter[LocalDate] = CustomLocalDateConverter
val data = DataSet.fromCsvFile("/tmp/data.csv")
data("date").extract[LocalDate]
```

Note that in either case the conversion set or custom converter can be passed in explicitly, but in the code above we
are setting it as an implicit value.

### Previewing Data

Once data is loaded into a `DataSet`, we can use the `preview()` method to print the first few lines to the console. The
number of lines to be displayed defaults to 10, but can be manually specified.


    ====================================================================
         id       name  age     dob      score  pass         time       
    ====================================================================
     12345678901  bob   19   2000-01-01  83.4   true   2018-01-01T12:00 
     23456789012  joe   29   1989-03-01  65.1   false  2018-01-02T14:00 
     34567890123  sam   25   1994-02-01  73.9   true   2018-01-03T08:00



