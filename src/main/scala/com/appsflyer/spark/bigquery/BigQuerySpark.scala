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

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryOutputFormat}
import com.google.gson.{JsonParser, JsonParseException}
import org.apache.spark.sql.{DataFrame, SQLContext}

object BigQuerySpark {

  /**
    * Enhanced version of SQLContext with BigQuery support.
    */
  implicit class BigQuerySQLContext(sqlContext: SQLContext) extends Serializable {

    @transient
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration

    /**
      * Set GCP project ID for BigQuery.
      */
    def setBigQueryProjectId(projectId: String): Unit = {
      hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    }

    /**
      * Set GCS bucket for temporary files.
      */
    def setBigQueryGcsBucket(gcsBucket: String): Unit = {
      hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)
    }
  }

  /**
    * Enhanced version of DataFrame with BigQuery support.
    */
  implicit class BigQueryDataFrame(df: DataFrame) extends Serializable {

    @transient
    val hadoopConf = df.sqlContext.sparkContext.hadoopConfiguration

    @transient
    val jsonParser = new JsonParser()

    /**
      * @param fullyQualifiedOutputTableId output-table id of the form
      *                                    [optional projectId]:[datasetId].[tableId]
      */
    def saveAsBigQueryTable(fullyQualifiedOutputTableId: String): Unit = {
      val adaptedDf = BigQueryAdapter(df)
      val tableSchema = BigQuerySchema(adaptedDf)

      BigQueryConfiguration.configureBigQueryOutput(hadoopConf, fullyQualifiedOutputTableId, tableSchema)
      hadoopConf.set("mapreduce.job.outputformat.class", classOf[BigQueryOutputFormat[_, _]].getName)

      adaptedDf.toJSON
        .rdd
        .map(json => (null, jsonParser.parse(json)))
        .saveAsNewAPIHadoopDataset(hadoopConf)
    }
  }
}
