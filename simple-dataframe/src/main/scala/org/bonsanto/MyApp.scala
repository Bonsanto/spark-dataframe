package org.bonsanto

object MyApp extends SparkApp {
  def main(args: Array[String]): Unit = {
    import sqlContext.implicits._


    val df = sc.textFile("E:\\Documents\\spark-dataframe\\variantspark\\src\\main\\scala\\resources\\rows.csv", 4)
      .map(line => {
        val values = line.split(";")
        (values.apply(0), values.apply(1))
      })
      .toDF("variant", "samples")

    df.show()
    println(df.count())
  }
}
