package spark.advance.training

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.SparkConf
import org.apache.hadoop.streaming.StreamXmlRecordReader
import org.apache.hadoop.mapred.JobConf
import scala.xml.XML

object Evaluator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Language Evaluator").setMaster("local")
    val sc = new SparkContext(conf)
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class",
      "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<page>")
    jobConf.set("stream.recordreader.end", "</page>")
    FileInputFormat.addInputPaths(jobConf, "file:///C:/Users/Sumit/git/SparkInAction/SparkOneStep/Data/WikiPages_BigData.xml")

    val wikiDocuments = sc.hadoopRDD(jobConf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[Text], classOf[Text])

    val deHadoopedWikis = wikiDocuments.map(hadoopXML => hadoopXML._1.toString)

    val rawWikiPages = deHadoopedWikis.map(wikiString => {
      val wikiXML = XML.loadString(wikiString)
      val wikiPageText = (wikiXML \ "revision" \ "text").text
      WikiCleaner.parse(wikiPageText)
    })

    val tokenizedWikiData = rawWikiPages.flatMap(wikiText => wikiText.split("\\W+"))

    println("CountofDocs    " + rawWikiPages.count)

    println("Distinct Word Count    " + tokenizedWikiData.distinct.count)
    
    println("Count Approximate Distinct   " + tokenizedWikiData.countApproxDistinct(.01))
    
    
    val mapValue  = tokenizedWikiData.countByValue()
   println("count by value" + mapValue)
   
   val maxvalue = mapValue.maxBy(x => x._2  )
   
   println("maxvalue  " + maxvalue)
    
   
   
    

    /*    val pertinentWikiData = tokenizedWikiData
      .map(wikiToken => wikiToken.replaceAll("[.|,|'|\"|?|)|(]", "").trim)
      .filter(wikiToken => wikiToken.length > 2)

    val wikiDataSortedByLength = pertinentWikiData.distinct
      .sortBy(wikiToken => wikiToken.length, ascending = false)
      .sample(withReplacement = false, fraction = .01)
      .keyBy { wikiToken => wikiToken.length }

    wikiDataSortedByLength.collect.foreach(println) */

  }
}