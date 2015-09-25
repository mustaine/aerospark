package com.osscube.spark.aerospike

import com.aerospike.client.AerospikeClient
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class AeroUDFSpec extends FlatSpec with Matchers with BeforeAndAfter {

   var client: AerospikeClient = _
   before {
     client = AerospikeInstance.aerospikeClient
   }

   after {
     client.close()
   }

   behavior of "UDF"

   it should "statement with UDF" in {

   }



 }
