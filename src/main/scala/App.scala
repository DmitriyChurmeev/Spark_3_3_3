import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{asc, col, current_date, desc, grouping, lit, max, min, regexp_replace, sum, to_date, when, year}

object App extends Context {

  override val appName: String = "Spark_3_3_3"

  def main(args: Array[String]) = {

    /**
     * Parse date column use yyyy-MM-dd/yyyy MM dd/yyyy MMM dd patterns
     * @param column column of date
     * @return parsed date column
     */
    def parseDate(column: Column): Column = {
      when(to_date(column).isNotNull, to_date(column))
        .otherwise(when(to_date(column, "yyyy MM dd").isNotNull, to_date(column, "yyyy MM dd"))
          .otherwise(when(to_date(column, "yyyy MMM dd").isNotNull, to_date(column, "yyyy MMM dd"))))
    }


    val carDf = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv("src/main/resources/cars.csv")

    import spark.implicits._
    val carDs = carDf.as[Car]

    val mileageSum = carDs.map(_.mileage.getOrElse(0.0)).reduce(_ + _)

    carDs
      .withColumn("avg_mileage", lit(mileageSum))
      .withColumn("years_since_purchase", lit(year(current_date()).minus(year(parseDate(col("date_of_purchase"))))))
      .show()
  }

}




