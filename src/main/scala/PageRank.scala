import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: PageRank InputDir, Maximum Iteration, OutputDir")
    }
    //Added in class
    val spark = SparkSession
      .builder()
      .appName("Spark PageRank")
      .getOrCreate()

    //Create Spark context with Spark configuration
    val conf = new SparkConf().setAppName("Spark PageRank").set("spark.driver.allowMultipleContexts", "true")
    val sc2 = SparkContext.getOrCreate(conf)
    //    val sc2 = new SparkContext(conf)

    //Get data from file and filter it
    //e.g. Array("9E","ATL","Atlanta, GA","GTR","Columbus, MS",, "9E","ATL","Atlanta, GA","GTR","Columbus, MS")
    val input = sc2.textFile(args(0))
    val header = input.first
    val filter = input.filter(x => x != header)

    //Get Origins and Destinations of flights
    val words = filter.map(x => (x.split(",")(1),x.split(",")(4)))
    val Ocount = words.map(x => (x._1,1.0))
    val Dcount = words.map(x => (x._2,1.0))

    //Join Origins and Destiantions of the airport list to get the total airport number
    val O_total_out = Ocount.reduceByKey((x,y) => x+y)
    val D_total_out = Dcount.reduceByKey((x,y) => x+y)
    val N = O_total_out.join(D_total_out).count()

    //count from A to B airline numbers
    val wordsOrigin = words.map(x => (x,1))
    val count = wordsOrigin.reduceByKey((x,y) => x+y)

    // Change the structure of Data, set Origin airport as the key, (Destinaion, count) as the value
    val OtoD = count.map(x => (x._1._1,(x._1._2,x._2.toDouble)))

    // Construct the graph of flights with weights, set Origin as the key, (Destination, weight) as the value
    val graph = OtoD.join(O_total_out).map(x => (x._1,(x._2._1._1,x._2._1._2/x._2._2)))

    val alpha = 0.15
    // initialize every airport to have pagerank of 10.0
    var pr = graph.map(x => (x._1,10.0))

    val pr_1 = alpha / N
    val iterations = args(1).toInt

    var i = 0;
    for( i <- 1 to iterations){

      var tmp_rank = graph.join(pr)
      var line_rate = tmp_rank.map(x => (x._2._1._1,(x._2._1._2 * x._2._2)))
      var pr_2 = line_rate.reduceByKey((x,y) => (x + y))

      pr = pr_2.map(x => (x._1, pr_1 + (1 - alpha) * x._2))
    }
    val ans = pr.sortBy (-_._2)
    ans.saveAsTextFile(args(2))

  }
}