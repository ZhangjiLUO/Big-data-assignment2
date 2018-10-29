import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD
import breeze.numerics.log

object MoviePlot {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    if (args.length != 2) {
      println("Usage: inputDir term_to_be_searched")
    }
    val inputDir = args(0)
    val targetWord = args(1)
    val plotFilePath = inputDir + "/plot_summaries.txt"
    val movieMetaFilePath = inputDir + "/movie.metadata.tsv"

    val corpus = sc.textFile(plotFilePath).map(_.toLowerCase)
    val stopWords = StopWordsRemover.loadDefaultStopWords("english").toSet

    val line = corpus.map(x => (x.split("""\t""")(0), x.split("""\t""")(1).split("""\W+""").filter(y => y.length > 1)))

    val inputRDDv = line.mapValues(y => y.filter(token => !stopWords.contains(token)).
      filter(_.forall(java.lang.Character.isLetter)))

    val plot = inputRDDv.flatMap(x => (x._2.map((_,(x._1,1))))).map(x => ((x._1,x._2._1),x._2._2))

    val termF = plot.reduceByKey((x, y) => (x + y))

    val docF = termF.map(x => ((x._1._1), 1)).reduceByKey((x, y) => (x + y))

    val joined = docF.join(termF.map{case(x, y) => ((x._1),(x._2, y))}).distinct.map{case(x, y) => ((x, y._2._1), (y._1, y._2._2))}

    val allDocs = line.count
    val res = joined.map{case(x, y) => (x, y._2 * log(allDocs / y._1.toDouble))}.map{case(x, y) => ((x._1),(x._2,y))}


    val resForTG = res.filter(_._1 == targetWord).sortBy(-_._2._2).map{case(x, y)=>(y._1, y._2)}


    val name = sc.textFile(movieMetaFilePath).
      map(x => (x.split("\t")(0), x.split("\t")(2)))

    val output = resForTG.join(name).map{case(x, y)=>(y._2, y._1)}.sortBy(-_._2).collect

    for(i <- 0 to output.length - 1){
      println(output(i)._1)
    }
  }

}
