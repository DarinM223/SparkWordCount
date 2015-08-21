import java.io.{File, PrintWriter}
import java.nio.file.{Paths, Files}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Sample Spark job that counts words in a text file
 * The "hello, world" of distributed computing :P
 */
object WordCountApp {

  def inputToFile(is: java.io.InputStream, f: java.io.File): Unit = {
    val in = scala.io.Source.fromInputStream(is)
    val out = new PrintWriter(f)
    try {
      in.getLines().foreach(out.print(_))
    } finally {
      out.close()
    }
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Word Counting App")
    val sc = new SparkContext(conf)

    val inputFilePath = "dracula.txt"
    val outputFilePath = "counts"

    // If input file doesn't exist locally, stream it from the JAR to the file
    if (!Files.exists(Paths.get(inputFilePath))) {
      val draculaStream = getClass().getResourceAsStream(inputFilePath)
      val draculaFile = new File(inputFilePath)
      inputToFile(draculaStream, draculaFile)
    }

    // Delete output path if it already exists
    if (Files.exists(Paths.get(outputFilePath))) {
      FileUtils.deleteDirectory(new File(outputFilePath))
    }

    // Create RDD from input text file with 8 partitions
    val input = sc.textFile(inputFilePath, 8)

    // Separate into words and cache into memory (instead of having to recompute)
    val words = input.flatMap(line => line.split(" ")).cache()

    // Transform into pairs of (key, value) sorted by value with the key being a
    // lowercase word with all non-alphanumeric letters stripped out
    // and the value being the count of the word
    // and then reduce by adding up values with the same key
    val counts = words.map({ word =>
      (word.replaceAll("[^A-Za-z0-9]", "").toLowerCase, 1)
    }).reduceByKey(_ + _).map(_.swap).sortByKey(false, 1).map(item => item.swap)

    // Merge into one partition and output to text file
    counts.coalesce(1).saveAsTextFile(outputFilePath)
  }
}
