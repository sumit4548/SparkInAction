package spark.advance.training

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.SparkConf



object Evaluater {
  def main(args: Array[String]){
    val conf =   new SparkConf().setAppName("Language Evaluator")
    val sc = new SparkContext(conf)
    val jobconf = new JobConf()
    jobconf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobconf.set("stream.recordreader.begin","<page>")
    jobconf.set("stream.recordreader.end","</page>")
    
    FileInputFormat.addInputPaths(jobconf, "file:///Data/WikiPages_BigData.xml")
    
    val wikiDocuments = sc.hadoopRDD(jobconf,classOf[org.apache.hadoop.streaming.StreamInputFormat],
    classOf[Text],classOf[Text]    
    )
  }
  
}