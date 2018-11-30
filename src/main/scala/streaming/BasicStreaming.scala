package streaming

import java.net.InetAddress

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object BasicStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingNetworkWordcount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("./data/streaming/checkpoint_location")

    val lines = ssc.socketTextStream(InetAddress.getLocalHost.getHostAddress, 9999)
    val counts = lines.flatMap(_.split(" ")).map(words => (words, 1)).updateStateByKey(updateCount _)
    counts.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def updateCount(newValues: Seq[Int], lastSum: Option[Int]): Option[Int] = {
    Some(newValues.size + lastSum.getOrElse(0))
  }

}
