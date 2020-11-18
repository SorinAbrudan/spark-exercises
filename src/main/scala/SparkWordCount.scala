/** Import the spark and math packages */
import org.apache.spark.SparkContext

object SparkWordCount {

  def main(args: Array[String]): Unit = {
    val inpath = "src/main/resources/all-shakespeare.txt"
    val outpath = "src/main/resources/words-count"

    val sc = new SparkContext("local[*]", "Word Count")

    val textFile = sc.textFile(inpath)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.saveAsTextFile(outpath)

    sc.stop()
  }
}