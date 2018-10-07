//HW2_Jie He

//import spark classes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

//define scala entry point
object WordCount {
  def main(args: Array[String]) {
    //initialise spark context
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)    

    //WordCount program
    
    //load both text files
    val file_1 = sc.textFile("one.txt")
    val file_2 = sc.textFile("two.txt")
    //split the text based on space
    val temp_1 = file_1.flatMap(x => x.split(" "))
    val temp_2 = file_2.flatMap(x => x.split(" "))
    //clean both text files. use Regex to remove special characters, transfer all letters to lowercase, and
    //eliminate empty strings
    val clean_1 = temp_1.map(_.replaceAll("[{[0-9],~,!,@,#,$,%,^,&,*,(,),{,},[,],_,=,-,:,',?,/,<,>,.\\[\\]}]", "").trim.toLowerCase).filter(!_.isEmpty)
    val clean_2 = temp_2.map(_.replaceAll("[{[0-9],~,!,@,#,$,%,^,&,*,(,),{,},[,],_,=,-,:,',?,/,<,>,.\\[\\]}]", "").trim.toLowerCase).filter(!_.isEmpty)
    
    val total_1 = clean_1.count
    println("the total number of words in one.txt: " + total_1)
    val unique_1 = clean_1.distinct.count
    println("the total number of unique words in one.txt: " + unique_1)
    
    //get the word counts for both files
    val counts_1 = clean_1.map(word => (word, 1))
    val counts_2 = clean_2.map(word => (word, 1))
    //combine two RDDs and eliminate duplicates
    var counts = counts_1.union(counts_2).reduceByKey(_+_).sortByKey().sortBy(_._2,false)
    
    //save the output
    counts.saveAsTextFile("wcOutput") 
    
    //terminate spark context
    //sc.stop()
    
  }
}

