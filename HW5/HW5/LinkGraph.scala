//HW5-Jie He
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.matching.Regex
import scala.collection.mutable.StringBuilder
import org.apache.hadoop.io.compress.GzipCodec

object LinkGraph {
  def main( args : Array[String] ) {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("LinkGraph")
    val sc = new SparkContext(sparkConf)    
    val input = sc.textFile("enwiki-articles")

    val page = input.map{ x => 
      val pair = x.stripPrefix( "(" )
                  .stripSuffix( ")" )
                  .split("\t", 2)
      (pair(0),pair(1))
    }

    val links = page.map(pair => ( pair._1, extractLinks(pair._2) ))                
    val linkCounts = links.map(pair => ( pair._1, pair._2.split("\t").length ))

    links.saveAsTextFile("links", classOf[GzipCodec])
    linkCounts.saveAsTextFile("links-counts", classOf[GzipCodec])
  }

  def extractLinks(text: String) : String = {
    var outlinkFinal = new StringBuilder
    val outlinkRegex = "\\[\\[.*?\\]\\]".r
    val outlinks = outlinkRegex findAllIn text

    outlinks.foreach { eachlink =>
      var temp = eachlink
      
      if (temp.contains(":")) {
        temp = ""
      }
      if (temp.contains("|")) {
        temp = temp.split("\\|")(0)
        temp = temp + "]]"
      }
      if (temp.contains("#")) {
        temp = temp.split("#")(0)
        temp = temp + "]]"
      }
      if (temp.contains(",")) {
        temp = temp.split(",")(0)
        temp = temp + "]]"
      }      
      if (temp.length == 4) {
        temp = ""
      }
      
      outlinkFinal.append(temp + "\t")
    }
    return outlinkFinal.toString
  }
}
