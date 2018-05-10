package com.ford.spark.ml.outlier

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.outlier.LOF


object LOF_Final{

  case class loftype(tablename: String, rowcnt: Long,  lof: Double, Tag_value: String)
  case class input(processed_date: String, processed_table: String, table_rowcnt: Long)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\Users\\yalla1\\Desktop\\Hadoop")


    val spark = SparkSession.builder().appName("LOFExample")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    def readExcel(file: String): DataFrame = spark.read
      .format("com.crealytics.spark.excel")
      .option("location", file)
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("addColorColumns", "False")
      .load()

    val customSchema = StructType(Array(
      StructField("processed_date", DataTypes.StringType, true),
      //StructField("processed_source", DataTypes.StringType, true),
      StructField("processed_table", DataTypes.StringType, true),
      //StructField("processed_column", DataTypes.StringType, true),
      StructField("table_rowcnt", DataTypes.LongType, true)
      //StructField("partsrc", DataTypes.StringType, true)
    ))

    val outputSchema = StructType(Array(
      StructField("processed_date", DataTypes.StringType, true),
      StructField("table", DataTypes.StringType, true),
      StructField("table_rowcnt", DataTypes.LongType, true),
      StructField("lof", DataTypes.DoubleType, true),
      StructField("tagval", DataTypes.StringType, true)
    ))


    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")

    val rawData = spark.read.schema(customSchema).option("delimiter","|").csv("C:\\Users\\yalla1\\Desktop\\Monitoring_List_RowCount.csv")


    val path="S:\\LOF\\AllReport"

    val outputpath="S:\\LOF\\Combination"

    val Data1 = rawData.select("processed_table","table_rowcnt")


    val grouped = Data1.filter(f=>(!f.isNullAt(1))).rdd.map(f => (f.getString(0), f.getLong(1))).groupBy(f => f._1)
    var mlist1 = List[(String, Double)]()



    val ldata = grouped.collect.foreach {
      line =>
        // if (!line._1.isEmpty && line._1 != null && line._1.nonEmpty) {
        print("Source Element  " + line._1)
        print("\n")

        var tablename = line._1

        import spark.sqlContext.implicits._
        val df = line._2.map(f => (f._2)).filter(f => (!f.equals(0))).toSeq.toDF("row_count").filter(col("row_count").isNotNull)
        if (df.distinct().count() > 1) {
          val ddf = df

          //val dk = ddf.show(1000, false)

          val assembler = new VectorAssembler()
            .setInputCols(ddf.columns)
            .setOutputCol("features")
          val data = assembler.transform(ddf)
          val result = new LOF()
            .setMinPts(5)
            .transform(data)
          result.count()
          var lof = 0.0
          val finald = result.map{ row =>
            //println(tablename +"  "+(row.get(2).toString.replace("[","").replace("]","")).toDouble.toLong +"  "+row.getDouble(1))
            val lof=row.getDouble(1)
            val Tag_value =(if(lof > 10) "Y" else "N")
            loftype(tablename, (row.get(2).toString.replace("[", "").replace("]", "")).toDouble.toLong, row.getDouble(1),Tag_value)
          }

          val fd = finald.toDF()
          // fd.show()
          //}

          val lofdata =rawData.join(fd,(rawData.col("processed_table") === fd.col("tablename")).
            and(rawData.col("table_rowcnt") === fd.col("rowcnt"))) .withColumn("table",concat(col("processed_table"),lit("."),col("processed_date")))
            .select("processed_date","table","table_rowcnt","lof","Tag_value")
          //lofdata.printSchema()
          //.filter(col("lof")>3)
          lofdata.distinct.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "false").csv(path+"/"+tablename)
          // lofdata.write.insertInto("lofdata")
          // lofdata.show()
        }
        else {
          val nonlofdata =rawData.filter(col("processed_table").equalTo(tablename)).withColumn("lof",lit(0.0))
            .withColumn("table",concat(col("processed_table"),lit("."),col("processed_date")))
            .withColumn("Tag_value",lit("")).withColumn("Tag_value",lit("no tag"))
            .select("processed_date","table","table_rowcnt","lof","Tag_value")

          //nonlofdata.printSchema()
          //nonlofdata.show()
          nonlofdata.distinct.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "false").csv(path+"/"+tablename)
          //nonlofdata.write.insertInto("lofdata")
        }
    }


     val finalDf=spark.read.schema(outputSchema).option("delimiter",",").csv(path+"/*")
     finalDf.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputpath)

  }
}
