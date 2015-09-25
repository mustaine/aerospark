package com.osscube.spark.aerospike.rdd

import org.scalatest.{FlatSpec, Matchers}

class DefaultSourceSpec extends FlatSpec with Matchers {


  behavior of "parse parameters"

  it should "use keyColumn when is in paramters" in {
    val key: String = "aKey"
    val host: String = "localhost:3000"
    val query: String = "select * from table"
    val params: Map[String, String] = Map(
      "initialHost" -> host,
      "select" -> query,
      "keyColumn" -> key)

    val defaultSource = new DefaultSource().createRelation(null, params)

    defaultSource shouldBe AeroRelation(host, query, 1, false, Some(key))(null)
  }

  it should "not use a keyColumn when is not in parameters" in {
    val key: String = "aKey"
    val host: String = "localhost:3000"
    val query: String = "select * from table"
    val params: Map[String, String] = Map(
      "initialHost" -> host,
      "select" -> query)

    val defaultSource = new DefaultSource().createRelation(null, params)

    defaultSource shouldBe AeroRelation(host, query, 1, false, None)(null)
  }
}