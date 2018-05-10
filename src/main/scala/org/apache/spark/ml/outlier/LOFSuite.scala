package org.apache.spark.ml.outlier

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object LOFSuite {

  case class loftype(val tablename:String,val rowcnt:Long, val lof: Double)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\Users\\yalla1\\Desktop\\Hadoop")
    val spark = SparkSession.builder().appName("LOFExample").master("local[4]").getOrCreate()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val customSchema = StructType(Array(
      StructField("processed_date", DataTypes.StringType, true),
      StructField("processed_source", DataTypes.StringType, true),
      StructField("processed_table", DataTypes.StringType, true),
      StructField("processed_column", DataTypes.StringType, true),
      StructField("table_rowcnt", DataTypes.LongType, true),
      StructField("partsrc", DataTypes.StringType, true)
    ))

    val rawData = spark.read.schema(customSchema).option("header", "true").csv("C:\\Users\\yalla1\\Desktop\\Table_count_LOF.csv")

    //rawData.show()

    val path="S:\\LOF\\Output\\"

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
        val df = line._2.map(f =>
          (f._2)
        ).filter(f => (!f.equals(0))).toSeq.toDF("row_count").filter(col("row_count").isNotNull)
        if (df.distinct().count() > 1) {
          val ddf=df
          val assembler = new VectorAssembler()
            .setInputCols(ddf.columns)
            .setOutputCol("features")
          val data = assembler.transform(ddf)
          val result = new LOF()
            .setMinPts(7)
            .transform(data)
          result.count()
          var lof = 0.0
          val finald=result.sort(desc(LOF.lof)).map { row =>
            //println(tablename +"  "+(row.get(2).toString.replace("[","").replace("]","")).toDouble.toLong +"  "+row.getDouble(1))
            loftype(tablename, (row.get(2).toString.replace("[","").replace("]","")).toDouble.toLong, row.getDouble(1))
          }

          val fd=finald.toDF()

          //  fd.show

          val lofdata=rawData.join(fd,(rawData.col("processed_table")===fd.col("tablename")).
            and(rawData.col("table_rowcnt")=== fd.col("rowcnt"))) .withColumn("table",concat(col("processed_table"),lit("."),col("processed_date")))
            .select("processed_date","table","table_rowcnt","lof")
          //.filter(col("processed_table").contains("dsc10036_cupid_lz_db.cut_cu15_merge__ct"))
          //.filter(col("lof")>3)

          //if(lofdata.count>1)
          // lofdata.distinct.coalesce(1).write.csv(path+"/"+tablename)

          //lofdata.write.insertInto("lofdata")
          lofdata.show
        }
        else {
          val nonlofdata=rawData.filter(col("processed_table").equalTo(tablename)).withColumn("lof",lit(0.0))
            .withColumn("table",concat(col("processed_table"),lit("."),col("processed_date")))
            .select("processed_date","table","table_rowcnt","lof")
          nonlofdata.show()
          // nonlofdata.write.csv(path+"/"+tablename)
          //nonlofdata.write.insertInto("lofdata")
        }
    }
  }
}