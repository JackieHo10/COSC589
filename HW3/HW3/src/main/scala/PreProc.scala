//HW3-Jie He
//using code template given by Grace
//import required libraries
import scala.io.Source
import java.io.PrintWriter
import java.io.File
import scala.collection.mutable.StringBuilder

object PreProc{
  def main(args: Array[String]){
    //define three input&output files
    //test on the small example provided
    //val inputfile = "wikidump.example"
    //val outputfile = new PrintWriter(new File("wikidump-example"))
    val inputfile = "enwiki-latest-pages-articles-multistream.xml"
    val outputfile = new PrintWriter(new File("enwiki-pages"))
    var a_output_line = new StringBuilder
    //initialize variables
    var counter = 0
    var flag = false
    //extract content and write into one line for each
    for(inputline <- Source.fromFile(inputfile)("UTF-8").getLines){
      //extract the starting point
      if(inputline.trim.startsWith("<page>")){
        counter = counter + 1
        flag = true
      }
      if(flag){
        a_output_line.append(inputline.trim)
      }
      //capture the ending point
      if(inputline.trim.startsWith("</page>")){
        //end of the content and reset the value of flag to start a new round of search
        flag = false
        //add the content to the output file in one line and clear the help variable to avoid duplicates
        outputfile.write(a_output_line.toString())
        outputfile.write("\n")
        a_output_line.clear()
      }
    }
    outputfile.close
    println("\n")
    println("total number of wiki pages: " + counter)
  }
}




