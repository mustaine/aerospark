package com.osscube.spark.aerospike.rdd

import java.util.UUID

import com.aerospike.client.{AerospikeClient, Bin, Key}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

class DataFrameFunction(data: DataFrame) extends Serializable {

  val sparkContext: SparkContext = data.rdd.sparkContext

  def saveToAerospike(initialHost: String,
                      namespace: String,
                      set: String) = {

    val splitHost = initialHost.split(":")
    val columns = data.columns

    println("columns: " + columns)
    data.rdd.foreachPartition { x =>
      val client = new AerospikeClient(null, splitHost(0), splitHost(1).toInt)
      x.foreach { s =>
        println("s: " + s)
        val row = s.toSeq
        if (row.length < columns.length) throw new IllegalArgumentException("wrong number of fields")
        val bins = for ((value, column) <- (row zip columns)) yield new Bin(column, value)
        client.put(null, new Key(namespace, set, UUID.randomUUID.toString), bins: _*)
      }
    }
  }
}

