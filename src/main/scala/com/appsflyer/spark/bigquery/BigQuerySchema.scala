/*
 * Copyright 2016 Appsflyer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.appsflyer.spark.bigquery

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}

/**
  * Builds BigQuery input JSON schema based on DataFrame.
  * Example schema can be found here: https://cloud.google.com/bigquery/docs/personsDataSchema.json
  */
object BigQuerySchema {

  private def getMode(field: StructField) = {
    field.dataType match {
      case ArrayType(_, _) => "repeated"
      case _ => if (field.nullable) "nullable" else "required"
    }
  }

  private def getTypeName(dataType: DataType) = {
    dataType match {
      case ByteType | ShortType | IntegerType | LongType => "integer"
      case FloatType | DoubleType => "float"
      case _: DecimalType | StringType => "string"
      case BinaryType => "bytes"
      case BooleanType => "boolean"
      case TimestampType => "timestamp"
      case ArrayType(_, _) | MapType(_, _, _) | _: StructType => "record"
    }
  }

  private def typeToJson(field: StructField, dataType: DataType): JValue = {
    dataType match {
      case structType: StructType =>
        ("type" -> getTypeName(dataType)) ~
          ("fields" -> structType.fields.map(fieldToJson(_)).toList)
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: ArrayType =>
            throw new IllegalArgumentException(s"Multidimensional arrays are not supported: ${field.name}")
          case other =>
            ("type" -> typeToJson(field, other))
        }
      case mapType: MapType =>
        throw new IllegalArgumentException(s"Unsupported type: ${dataType}")
      case other =>
        ("type" -> getTypeName(dataType))
    }
  }

  private def fieldToJson(field: StructField): JValue = {
    ("name" -> field.name) ~
      ("mode" -> getMode(field)) merge
      typeToJson(field, field.dataType)
  }

  def apply(df: DataFrame): String = {
    pretty(render(JArray(df.schema.fields.map(fieldToJson(_)).toList)))
  }
}
