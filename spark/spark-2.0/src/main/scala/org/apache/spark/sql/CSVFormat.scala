/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import execution.datasources.csv.CSVOptions
import execution.datasources.csv.CSVRelation
import execution.datasources.csv.LineCsvReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

private[spark] object CSVFormat {

  def buildReader(
      sparkSession: SparkSession,
      schema: StructType,
      options: Map[String, String]): Iterator[Array[Byte]] => Iterator[InternalRow] = {
    val csvOptions = new CSVOptions(options)

    new (Iterator[Array[Byte]] => Iterator[InternalRow]) with Serializable {
      lazy val lineReader = new LineCsvReader(csvOptions)
      lazy val rowParser = CSVRelation.csvParser(schema, schema.fieldNames, csvOptions)

      override def apply(records: Iterator[Array[Byte]]): Iterator[InternalRow] = {
        records.flatMap { r =>
          val recordStr = new String(r, csvOptions.charset)
          val line = lineReader.parseLine(recordStr)
          if (line != null) rowParser(line, 0) else None
        }
      }
    }
  }
}
