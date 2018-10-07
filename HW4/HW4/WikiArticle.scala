//HW4-Jie He
//using the code template from the assignment
//import library
import scala.util.matching.Regex
import scala.xml.XML
import scala.io.Source
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.StringBuilder
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WikiArticle{
  def main(args: Array[String]){
    val SparkConf = new SparkConf().setMaster("local[1]").setAppName("WikiArticle")
    val sc = new SparkContext(SparkConf)
    val txt = sc.textFile("smalloutput") //load in the output from HW3
    
    val getTitleAndText = txt.map{l => 
      val line = XML.loadString(l)
      val title = (line \ "title").text
      //code below to get the strings in <text></text> and output the title and the text
      val text = (line \\ "text").text //deep search 
      (title, text) //output the pair
    }
    val articles = getTitleAndText.filter{r=>isArticle(r._2.toLowerCase)} //convert text toLowerCase
    val articlesCounter = articles.count
    println("the article counter is " + articlesCounter)
    
    //save the articles based on the format in 4
    val savearticles = getTitleAndText.map{case (x, y) => x + "\t" +  y} //separate title and text by tab
    savearticles.saveAsTextFile("enwiki-articles")
  
    //perform WordCount below; using code from HW2
    //split the text based on space
    val temp_1 = savearticles.flatMap(x => x.split(" "))
    //clean the text file. use Regex to remove special characters, transfer all letters to lowercase, and
    //eliminate empty strings
    val clean_1 = temp_1.map(_.replaceAll("[{[0-9],~,!,@,#,$,%,^,&,*,(,),{,},[,],_,=,-,:,',?,/,<,>,.\\[\\]}]", "").trim.toLowerCase).filter(!_.isEmpty)
    
    val total_1 = clean_1.count
    println("the total number of words: " + total_1)
    val unique_1 = clean_1.distinct.count
    println("the total number of unique words: " + unique_1)
    
    //get the word count for the text file
    val counts_1 = clean_1.map(word => (word, 1))
    var counts = counts_1.reduceByKey(_+_).sortByKey().sortBy(_._2,false)
    
    //save the WC output
    counts.saveAsTextFile("Wiki-WC") 
    
    //terminate spark context; it might cause some connection errors.
    //sc.stop()
  }
  
  //a help function called isArticle to determine the type of Wiki pages
  def isArticle(line: String): Boolean = {
    val stubsregex = new Regex("""\-stub\}\}""")
    val redirectsregex = new Regex("""\#redirect""")
    val disambiguationregex = new Regex("""\{\{disambig\w*\}\}""")
    val disambiguationregex_1 = new Regex("""\(disambiguation\)""") //different disambiguation classifications
      
    val findstub = stubsregex findAllIn line
    val findredirects = redirectsregex findAllIn line 
    val finddisambiguation = disambiguationregex findAllIn line 
    val finddisambiguation_1 = disambiguationregex_1 findAllIn line
    
    return (findstub.mkString(" ").isEmpty && finddisambiguation.mkString(" ").isEmpty && findredirects.mkString(" ").isEmpty && finddisambiguation_1.mkString(" ").isEmpty)
  }//return a Boolean value based on Regex to identify articles
}
    
  