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

package com.appsflyer.spark.utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, DataType}

/**
  * Various Spark utilities and extensions of DataFrame
  * To use, import com.appsflyer.spark.DataFrameUtils._
  */
object DataFrameUtils {

  private def dropSubColumn(col: Column, colType: DataType, fullColName: String, dropColName: String): Option[Column] = {
    if (fullColName.equals(dropColName)) {
      None
    } else {
      colType match {
        case colType: StructType =>
          if (dropColName.startsWith(s"${fullColName}.")) {
            Some(struct(
              colType.fields
                .flatMap(f =>
                  dropSubColumn(col.getField(f.name), f.dataType, s"${fullColName}.${f.name}", dropColName) match {
                    case Some(x) => Some(x.alias(f.name))
                    case None => None
                  })
                : _*))
          } else {
            Some(col)
          }
        case other => Some(col)
      }
    }
  }

  protected def dropColumn(df: DataFrame, colName: String): DataFrame = {
    df.schema.fields
      .flatMap(f => {
        if (colName.startsWith(s"${f.name}.")) {
          dropSubColumn(col(f.name), f.dataType, f.name, colName) match {
            case Some(x) => Some((f.name, x))
            case None => None
          }
        } else {
          None
        }
      })
      .foldLeft(df.drop(colName)) {
        case (df, (colName, column)) => df.withColumn(colName, column)
      }
  }

  /**
    * Extended version of DataFrame that allows to operate on nested fields
    */
  implicit class ExtendedDataFrame(df: DataFrame) extends Serializable {
    /**
      * Drops nested field from DataFrame
      *
      * @param colName Dot-separated nested field name
      */
    def dropNestedColumn(colName: String): DataFrame = {
      DataFrameUtils.dropColumn(df, colName)
    }
  }
}
