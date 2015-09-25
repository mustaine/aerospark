/*
 * Copyright 2014 OSSCube UK.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.osscube.spark.aerospike

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.language.implicitConversions


package object rdd {


//  implicit class AeroDataFrameWriter(writer: DataFrameWriter) {
//    def aerospike: String => Unit = writer.format("com.osscube.spark.aerospike").save
//  }

  implicit def toRDDFunctions[T](rdd: RDD[T]): RDDFunctions[T] =
    new RDDFunctions(rdd)

  implicit def toDataFrameFunctions(data: DataFrame): DataFrameFunction =
    new DataFrameFunction(data)


  implicit def AeroContext(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)



  implicit class AeroSqlContext(sqlContext: SQLContext) {
    def aeroRDD(
                 initialHost: String,
                 select: String,
                 numPartitionsPerServerForRange : Int = 1) =
      new SparkContextFunctions(sqlContext.sparkContext).aeroSInput(initialHost, select,sqlContext, numPartitionsPerServerForRange )

  }
}
