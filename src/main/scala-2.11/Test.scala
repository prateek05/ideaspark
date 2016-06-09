/**
  * Created by baranwal on 6/9/16.
  */


import org.apache.spark.{SparkContext,SparkConf}

object Test {
  def main(args: Array[String]){
  val conf = new SparkConf().setAppName("DevDemo").setMaster("local")
  val sc = new SparkContext(conf)
    val inputFile = sc.textFile("/var/log/fsck_hfs.log").cache()
    // Creates a DataFrame having a single column named "line"
     val errAs = inputFile.filter(line => line.contains("ERROR"))
    println("Error count : %s".format(errAs.count()))
  }
}
