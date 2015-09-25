package com.osscube.spark.aerospike.rdd

import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
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

  behavior of "insert"

  it should "insert data" in {


  }
}
