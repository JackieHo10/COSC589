//HW7(Optional)-Jie He
import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast

object TopicPageRank {
    def main(args:Array[String]) {
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("TopicPageRank")
        val sc = new SparkContext(sparkConf)
        
        //load link graphs (using code from HW6)
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
        
        //load topic pages
        val football = sc.textFile("footballer.txt").map(a => "[[" + a + "]]").collect.toArray
        //create a broadcast variable to hold the football pages
        val topicPages = sc.broadcast(football.toSet)

        //Implement your Topic-Oriented PageRank algorithm
        val ITERATION = 10
        
        for (i <- 0 to ITERATION) {
          val contribs = links.join(ranks).flatMap {
            case (title, (links, rank)) =>
              links.map(dest => (dest, rank / links.size))
          }
          var onTopicRank = contribs.reduceByKey( _+_ ).filter(x => topicPages.value.contains(x._1)).mapValues(0.15 + 0.85 * _ )
          var offTopicRank = contribs.reduceByKey( _+_ ).filter(x => !topicPages.value.contains(x._1)).mapValues(0.85 * _ )
          ranks = onTopicRank.union(offTopicRank)
        }
        
        //Sort pages by their PageRank scores
        val ranks_ft_sorted = ranks.sortBy(_._2, false)
        
        //save the page title and pagerank scores in compressed format (save your disk space). Using "\t" as the delimiter.
        ranks_ft_sorted.map(r => r._1 + "\t" + r._2).saveAsTextFile("TopicPageRanks", classOf[GzipCodec])
    }
    
}







