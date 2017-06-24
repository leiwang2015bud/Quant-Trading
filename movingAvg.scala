// An example of implement of simple moving average method
// moving window size: 3
// moving step: 1
//code reference: https://stackoverflow.com/questions/31965615/moving-average-in-spark-java

// build a RDD with the numbers from 0 to 100 which are stored in different 10 partitions
val ts = sc.parallelize(0 to 100, 10)
val window = 3//moving average window

//1. duplicate the data at the start of the partitions, 
//so that calculating the moving average per partition giveds complete coverage.
class StraightPartitioner(p: Int) extends org.apache.spark.Partitioner {
  def numPartitions = p
  def getPartition(key: Any) =  key.asInstanceOf[Int]
}

//2. create the data with the first window-1 rows copied to the previous partition
val partitioned = ts.mapPartitionsWithIndex((i, p) => {
  val overlap = p.take(window - 1).toArray
  val spill = overlap.iterator.map((i - 1, _))
  val keep = (overlap.iterator ++ p).map((i, _))
  if (i == 0) keep else keep ++ spill
}).partitionBy(new StraightPartitioner(ts.partitions.length)).values

//3. calculate the sum for each window
val sumOfEachWindow = partitioned.mapPartitions(p => {
  val sorted = p.toSeq.sorted
  val olds = sorted.iterator
  val news = sorted.iterator
  var sum = news.take(window - 1).sum
  (olds zip news).map({ case (o, n) => {
    sum += n
    val v = sum
    sum -= o
    v
  }})
})

//make a simple test
println(sumOfEachWindow.collect.sameElements(3 to 297 by 3))

// the simple moving Average result where
// window size is 3
// moving step is 1
val movingAverage = sumOfEachWindow.collect.map(x=>x/window)

