package com.osscube.spark.aerospike.rdd

import com.aerospike.client.{AerospikeClient, Bin, Key}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RDDFunctions[T](rdd: RDD[T]) extends Serializable {

  val sparkContext: SparkContext = rdd.sparkContext

  def saveToAerospike(initialHost: String,
                      namespace: String,
                      set: String) = {

    val splitHost = initialHost.split(":")
    println("the rdd:")
    rdd.foreachPartition { x =>
      val client = new AerospikeClient(null, splitHost(0), splitHost(1).toInt)
      x.foreach { s =>
        println("s: " + s)
        client.put(null, new Key(namespace, set, s.toString()), new Bin("column1", s)
        )
      }
    }

    //    val writer = TableWriter(client, namespace, set)
    //    rdd.sparkContext.runJob(rdd, writer.write _)
    println("finally we are here!")
  }
}

