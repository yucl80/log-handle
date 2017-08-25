package yucl.learn.demo.log2hdfs

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object AvroIOScalaDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("sss")
      .setMaster("local[*]")
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    /*val df = Seq(
      (2017, 8, "/test/a.html", 200),
      (2017, 8, "/test/a.htm", 200),
      (2017, 7, "/test/b.htm", 404),
      (2017, 7, "/test/c.htm", 500)).toDF("year", "month", "uri", "code")*/

    //sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")
    //sqlContext.setConf("mapreduce.input.fileinputformat.split.maxsize","67108864")
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.split.maxsize","134217728")
    val filePath = "hdfs://10.62.14.67:8020/logs/acclog"

    // val name = "AvroTest"
    // val namespace = "yucl.learn.demo"
    // val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)
    // df.write.options(parameters).partitionBy("year", "month").avro("/tmp/a.avro")

   /* df.write.mode(SaveMode.Append).partitionBy("year", "month").avro(filePath)*/

    sqlContext.sql("CREATE TEMPORARY TABLE table_name USING com.databricks.spark.avro OPTIONS (path \"" + filePath + "\")")
    val df2 = sqlContext.sql("SELECT * FROM table_name where service='um' and stack='it-dev-20170810' and sessionid='19551B3CDE72D9B0B2E8080AF438DFA1'")
    df2.foreach { x => println(x) }

  /* val df1 = sqlContext.read.avro(filePath)
    df1.printSchema()

    df1.filter("year = 2017" )
      .filter("service = 'um'")
      .filter("stack = 'it-dev-20170810'")
      .filter("sessionid  = '19551B3CDE72D9B0B2E8080AF438DFA1'")
      .collect()
      .foreach(println)*/
  }
}