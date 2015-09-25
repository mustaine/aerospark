package com.osscube.spark.aerospike.rdd

import java.util

import com.aerospike.client.Record
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class AeroRelationSpec extends FlatSpec with Matchers {

  behavior of "schema"

  it should "build schema from data" in {

    val aeroRelation = new AeroRelation("localhost:3000", "select * from test.hundred", 1, false)(null)

    aeroRelation.schema shouldBe new StructType(Array(
      StructField("column1", StringType, true),
      StructField("column2", StringType, true),
      StructField("intColumn1", DataTypes.LongType, true)))
  }

  it should "build schema using bins from params with default type" in {

    val aeroRelation = new AeroRelation("localhost:3000", "select * from test.hundred", 1, false)(null)

    val bins: Map[String, AnyRef] = Map(
      "bin1" -> "aString",
      "bin2" -> 1.asInstanceOf[AnyRef],
      "bin3" -> 1L.asInstanceOf[AnyRef])

    val params = new QueryParams("namespace", "set", bins.keySet.toSeq, null, null, null, null)

    val record = new Record(new util.HashMap[String, Object](), 1, 1)
    aeroRelation.buildSchema(params, record) shouldBe new StructType(Array(
      StructField("bin1", StringType, true),
      StructField("bin2", StringType, true),
      StructField("bin3", StringType, true)))
  }

  it should "build schema using bins from query" in {

    val aeroRelation = new AeroRelation("localhost:3000", "select * from test.hundred", 1, false)(null)

    val bins: Map[String, AnyRef] = Map(
      "bin1" -> "aString",
      "bin2" -> 1.asInstanceOf[AnyRef],
      "bin3" -> 1L.asInstanceOf[AnyRef])

    val params = new QueryParams("namespace", "set", Seq("*"), null, null, null, null)

    import collection.JavaConversions._
    val record = new Record(mapAsJavaMap(bins), 1, 1)
    println(record)
    aeroRelation.buildSchema(params, record) shouldBe new StructType(Array(
      StructField("bin1", StringType, true),
      StructField("bin2", IntegerType, true),
      StructField("bin3", LongType, true)))
  }

  behavior of "insert"

  it should "insert data" in {


  }
}
