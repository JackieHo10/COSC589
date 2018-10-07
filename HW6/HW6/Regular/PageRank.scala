//HW6_Jie He
import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec

object PageRank{
  def main(args: Array[String]){
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("PageRank")
    val sc = new SparkContext(sparkConf)
    val input = sc.textFile("links/*.gz") //output directory from HW5
    //Load RDD of (page title, links) pairs
    val links = input.map{line =>
      var str = line.stripPrefix("(").stripSuffix(")").split(",", 2)
      val title = "[[" + str(0) + "]]";
      val neighborlinks = str(1).trim.split("\t")
      (title, neighborlinks)
    }
    //Load RDD of (page title, rank) pairs
    var ranks = links.map{r => (r._1, 1.0)}
    
    val ITERATION = 20
    //Implement your PageRank algorithm according to the notes
    for (i <- 0 to ITERATION) {
      var contribs = links.join(ranks).flatMap {
        case (title, (links, rank)) => 
        links.map(dest => (dest, rank / links.size))
      }
      ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85 * _ )
    }
    //Sort pages by their PageRank scores
    val filtered = ranks.filter(x => !x._1.contains(':'))
    val filtered_1 = filtered.filter(y => !y._1.isEmpty)
    val rank = filtered_1.sortBy(_._2, false)
    
    //save the page title and pagerank scores in compressed format (save your disk space). Using "\t" as the delimiter.
    rank.map(r => r._1 + "\t" + r._2).saveAsTextFile("PageRank", classOf[GzipCodec])
    
  }
}








