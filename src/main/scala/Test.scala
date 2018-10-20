import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

  object Test {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Time Usage")
        .config("spark.master", "local")
        .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    def main(args: Array[String]): Unit = {
      timeUsageByLifePeriod()
    }

    def timeUsageByLifePeriod(): Unit = {
      val (columns, initDf) = read("/timeusage/atussum_test.csv")
      val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
//      println(primaryNeedsColumns, workColumns, otherColumns)
      val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
//      summaryDf.show()
//      val finalDf = timeUsageGrouped(summaryDf)
//      val finalDf = timeUsageGroupedSql(summaryDf)
      val typedDf = timeUsageSummaryTyped(summaryDf)
      val finalDf = timeUsageGroupedTyped(typedDf)
      finalDf.show()
    }

    def read(resource: String): (List[String], DataFrame) = {
      val rdd = spark.sparkContext.textFile(fsPath(resource))

      val headerColumns = rdd.first().split(",").to[List]
      // Compute the schema based on the first line of the CSV file
      val schema = dfSchema(headerColumns)
//
//      schema.printTreeString()
      val data =
        rdd
          .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
          .map(_.split(",").to[List])
          .map(row)

      val dataFrame =
        spark.createDataFrame(data, schema)

//      dataFrame.show()

      (headerColumns, dataFrame)
    }

    def fsPath(resource: String): String =
      Paths.get(getClass.getResource(resource).toURI).toString

    def dfSchema(columnNames: List[String]): StructType =
      StructType(columnNames.map(column => {
        if(column == "tucaseid") StructField(column, StringType, false)
        else StructField(column, DoubleType, false)
      }))

    def row(line: List[String]): Row =
      Row.fromSeq(line.zipWithIndex.collect {
        case (element, index) if index == 0 => element
        case (element, index) => element.toDouble
      })

    def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
      val basicList = List("t01", "t03", "t11", "t1801", "t1803")
      val activityList = List("t05", "t1805")
      val otherList = List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16","t18")

      val primary = for{
        name <- columnNames
        if basicList.exists(a => name.startsWith(a))
      } yield col(name)

      val activity = for{
        name <- columnNames
        if activityList.exists(a => name.startsWith(a))
      } yield col(name)

      val leisure = for{
        name <- columnNames
        if otherList.exists(a => name.startsWith(a))
      } yield col(name)

      (primary, activity, leisure)
    }

    /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
      *         are summed together in a single column (and same for work and leisure). The “teage” column is also
      *         projected to three values: "young", "active", "elder".
      *
      * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
      * @param workColumns List of columns containing time spent working
      * @param otherColumns List of columns containing time spent doing other activities
      * @param df DataFrame whose schema matches the given column lists
      *
      * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
      * a single column.
      *
      * The resulting DataFrame should have the following columns:
      * - working: value computed from the “telfs” column of the given DataFrame:
      *   - "working" if 1 <= telfs < 3
      *   - "not working" otherwise
      * - sex: value computed from the “tesex” column of the given DataFrame:
      *   - "male" if tesex = 1, "female" otherwise
      * - age: value computed from the “teage” column of the given DataFrame:
      *   - "young" if 15 <= teage <= 22,
      *   - "active" if 23 <= teage <= 55,
      *   - "elder" otherwise
      * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
      * - work: sum of all the `workColumns`, in hours
      * - other: sum of all the `otherColumns`, in hours
      *
      * Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
      *
      * Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
      */
    def timeUsageSummary(
                          primaryNeedsColumns: List[Column],
                          workColumns: List[Column],
                          otherColumns: List[Column],
                          df: DataFrame
                        ): DataFrame = {
      // Transform the data from the initial dataset into data that make
      // more sense for our use case
      // Hint: you can use the `when` and `otherwise` Spark functions
      // Hint: don’t forget to give your columns the expected name with the `as` method
      val workingStatusProjection: Column = functions.when(
        col("telfs") >= 1 && col("telfs") < 3, "working"
      ).otherwise("not working").as("working")
      val sexProjection: Column = functions.when(col("tesex") === 1, "male").otherwise("female").as("sex")
      val ageProjection: Column = functions.when(col("teage") >= 15 && col("teage") <= 22, "young")
        .when(col("teage") >= 23 && col("teage") <= 55, "active")
        .otherwise("elder").as("age")

      // Create columns that sum columns of the initial dataset
      // Hint: you want to create a complex column expression that sums other columns
      //       by using the `+` operator between them
      // Hint: don’t forget to convert the value to hours
      val primaryNeedsProjection: Column = primaryNeedsColumns.reduce(_+_)./(60).as("primaryNeeds")
      val workProjection: Column = workColumns.reduce(_+_)./(60).as("work")
      val otherProjection: Column = otherColumns.reduce(_+_)./(60).as("other")
      df
        .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
        .where($"telfs" <= 4) // Discard people who are not in labor force
    }

    /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
      *         ages of life (young, active or elder), sex and working status.
      * @param summed DataFrame returned by `timeUsageSumByClass`
      *
      * The resulting DataFrame should have the following columns:
      * - working: the “working” column of the `summed` DataFrame,
      * - sex: the “sex” column of the `summed` DataFrame,
      * - age: the “age” column of the `summed` DataFrame,
      * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
      *   status, sex and age, rounded with a scale of 1 (using the `round` function),
      * - work: the average value of the “work” columns of all the people that have the same working status, sex
      *   and age, rounded with a scale of 1 (using the `round` function),
      * - other: the average value of the “other” columns all the people that have the same working status, sex and
      *   age, rounded with a scale of 1 (using the `round` function).
      *
      * Finally, the resulting DataFrame should be sorted by working status, sex and age.
      */
    def timeUsageGrouped(summed: DataFrame): DataFrame = {
      val avgPrimaryNeeds = summed.groupBy(col("age"), col("sex"), col("working"))
        .agg(functions.round(functions.avg("primaryNeeds"), 1).as("primaryNeedsAvg"),
          functions.round(functions.avg("work"), 1).as("workAvg"),
          functions.round(functions.avg("other"), 1).as("otherAvg"))
//      avgPrimaryNeeds.show()
      avgPrimaryNeeds.select(col("working"), col("sex"), col("age"),
        col("primaryNeedsAvg"), col("workAvg"), col("otherAvg"))
        .orderBy("working", "sex", "age")
    }

    /**
      * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
      * @param summed DataFrame returned by `timeUsageSumByClass`
      */
    def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
      val viewName = s"summed"
      summed.createOrReplaceTempView(viewName)
      spark.sql(timeUsageGroupedSqlQuery(viewName))
    }

    /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
      * @param viewName Name of the SQL view to use
      *                 grouped by the different
      * grouped by the different ages of life (young, active or elder), sex and working status.
      * Finally, the resulting DataFrame should be sorted by working status, sex and age.
      */
    def timeUsageGroupedSqlQuery(viewName: String): String =
      "select working, sex, age, " +
        "round(avg(primaryNeeds),1) primaryNeedsAvg, round(avg(work),1) workAvg, round(avg(other),1) otherAvg " +
        "from summed " +
        "group by age, sex, working " +
        "order by working, sex, age "

    def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
      timeUsageSummaryDf.map(r => TimeUsageRow(
        r.getAs[String]("working"), r.getAs[String]("sex"), r.getAs[String]("age"),
        r.getAs[Double]("primaryNeeds"), r.getAs[Double]("work"), r.getAs[Double]("other")
      ))

    /**
      * @return Same as `timeUsageGrouped`, but using the typed API when possible
      * @param summed Dataset returned by the `timeUsageSummaryTyped` method
      *
      * grouped by the different ages of life (young, active or elder), sex and working status.
      * Finally, the resulting DataFrame should be sorted by working status, sex and age.
      *
      *
      * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
      * dataset contains one element per respondent, whereas the resulting dataset
      * contains one element per group (whose time spent on each activity kind has
      * been aggregated).
      *
      *
      * Hint: you should use the `groupByKey` and `typed.avg` methods.
      *
      *
      */
    def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
      import org.apache.spark.sql.expressions.scalalang.typed
      val groupedDS = summed.groupByKey(t => (t.age, t.sex, t.working))
        .agg(round(typed.avg[TimeUsageRow](_.primaryNeeds), 1).as("primaryNeedsAvg").as[Double],
          round(typed.avg[TimeUsageRow](_.work), 1).as("workAvg").as[Double],
          round(typed.avg[TimeUsageRow](_.other), 1).as("otherAvg").as[Double])
        .map{ case ((age, sex, working), primaryNeeds, work, other) => TimeUsageRow(working, sex, age, primaryNeeds,work, other)}
        .orderBy(col("working"), col("sex"), col("age"))

      groupedDS

//      groupedDS.show()

//      groupedDS.select(col("working"), col("sex"), col("age"),
//          col("primaryNeedsAvg"), col("workAvg"), col("otherAvg"))
//        .orderBy(col("working"), col("sex"), col("age")).as[TimeUsageRow]
    }

    case class TimeUsageRow(
       working: String,
       sex: String,
       age: String,
       primaryNeeds: Double,
       work: Double,
       other: Double
     )
  }