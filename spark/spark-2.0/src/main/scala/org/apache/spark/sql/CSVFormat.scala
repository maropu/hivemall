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
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

private[spark] object CSVFormat extends Logging {

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


  def buildReader1(
      sparkSession: SparkSession,
      schema: StructType,
      options: Map[String, String]): Iterator[Array[Byte]] => Iterator[InternalRow] = {
    val csvOptions = new CSVOptions(options)

    new (Iterator[Array[Byte]] => Iterator[InternalRow]) with Serializable {
      lazy val lineReader = new LineCsvReader(csvOptions)
      lazy val rowParser = CSVRelation.csvParser(schema, schema.fieldNames, csvOptions)

      override def apply(records: Iterator[Array[Byte]]): Iterator[InternalRow] = {
        var allTime: Long = System.nanoTime()
        var readTime: Long = 0
        var parseTime: Long = 0
        var flatTime: Long = 0
        def close(): Unit = {
          val tc = TaskContext.get()
          logInfo(s"StageID:${tc.stageId} TaskID:${tc.taskAttemptId()} readTime:${readTime} " +
            s"parseTime:${parseTime} all:${(System.nanoTime() - allTime) / 1000000}" +
          s"flatTime:${flatTime}")
        }
        val result = records.map { r =>
          val start3 = System.nanoTime
          val recordStr = new String(r, csvOptions.charset)
          val start1 = System.nanoTime
          val a = lineReader.parseLine(recordStr)
          readTime += ((System.nanoTime() - start1) / 1000000)
          val res = if (a != null) {
            if (a.size > 33) {
              val start2 = System.nanoTime
              val r = rowParser(
                Array(a(1), a(21), a(23), a(24), a(26), a(27), a(29), a(30), a(32), a(33)),
                0)
              parseTime += ((System.nanoTime() - start2) / 1000000)
              r
            } else {
              None
            }
          } else {
            None
          }
          flatTime += ((System.nanoTime() - start3) / 1000000)
          res
        }
        val d = CompletionIterator[Option[InternalRow], Iterator[Option[InternalRow]]](
          result, close())
        var count = 0
        for (i <- d) {
          count += 1
        }
        println("cont:" + count)
        Iterator.empty
      }
    }
  }

  def buildReader2(
      sparkSession: SparkSession,
      schema: StructType,
      options: Map[String, String]): Iterator[Array[Byte]] => Iterator[InternalRow] = {
    val csvOptions = new CSVOptions(options)

    new (Iterator[Array[Byte]] => Iterator[InternalRow]) with Serializable {
      lazy val lineReader = new LineCsvReader(csvOptions)
      lazy val rowParser = CSVRelation.csvParser(schema, schema.fieldNames, csvOptions)

      override def apply(records: Iterator[Array[Byte]]): Iterator[InternalRow] = {
        var allTime: Long = System.nanoTime()
        var readTime: Long = 0
        var parseTime: Long = 0
        def close(): Unit = {
          val tc = TaskContext.get()
          logInfo(s"StageID:${tc.stageId} TaskID:${tc.taskAttemptId()} readTime:${readTime} " +
            s"parseTime:${parseTime} all:${((System.nanoTime() - allTime) / 1000000)}")
        }
        val d = CompletionIterator[Array[Byte], Iterator[Array[Byte]]](records, close())
        var count = 0
        for (i <- d) {
          count += 1
        }
        println("cont:" + count)
        Iterator.empty
      }
    }
  }
}
