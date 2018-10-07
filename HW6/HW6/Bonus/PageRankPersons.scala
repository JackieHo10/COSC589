//HW6_Jie He
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.matching.Regex
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.compress.GzipCodec

object PageRankPersons {
	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setMaster("local[1]").setAppName("PageRankPersons")
 		val sc = new SparkContext(sparkConf)
    //build a filter to select articles only associated with persons
		val articles = sc.textFile("enwiki-articles")	
		//search by regex
		val pattern = """\{\{Infobox\s+person(.*?)\}\}""".r
		val people = articles.filter(line => pattern.findFirstIn(line).toList.length > 0)
		//extract titles only associated with persons
		val persontitles = people.map{ a =>
			val pairs = a.stripPrefix("(").stripSuffix(")").split("\t", 2)
			"[[" + pairs(0) + "]]"
		}
    //create RDDs to match by key
    val pairRDD = persontitles.map(b => (b, 1.0))
    val PageRanks = sc.textFile("PageRank/*.gz")
		val pagerank = PageRanks.map{c =>
			val Case = c.split("\t")
			val pagetitle = Case(0).trim
			val rankscore = Case(1).trim
			(pagetitle, rankscore)
		}
    //match by key
    val rankpersons = pagerank.join(pairRDD)
    val rankpersons_update = rankpersons.map(d => (d._1,  d._2._1.toDouble)).distinct()
    val ranksnew = rankpersons_update.sortBy(_._2, false)
    //output the results in .gz format
    ranksnew.map(e => e._1 + "\t" + e._2).saveAsTextFile("PersonsRanks", classOf[GzipCodec])
	}
}