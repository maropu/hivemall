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

package org.apache.spark.sql.execution.joins

import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric._
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.util.CompletionIterator
import utils.InternalRowPriorityQueue

case class TopKShuffledHashJoinExec(
    k: Int,
    scoreExpression: Expression,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryExecNode with HashJoin {
  type QueueType = (AnyRef, InternalRow)

  require(k != 0, "`k` must not have 0")

  private[this] var startTime: Long = 0
  private[this] var _numProcessedRows: Long = 0
  private[this] var _numInputRows: Long = 0
  private[this] var _numOutputRows: Long = 0

  override val joinType: JoinType = Inner

  private[this] lazy val scoreType = scoreExpression.dataType
  private[this] lazy val scoreOrdering = {
    val ordering = TypeUtils.getInterpretedOrdering(scoreType)
      .asInstanceOf[Ordering[AnyRef]]
    if (k > 0) {
      ordering
    } else {
      ordering.reverse
    }
  }

  private[this] lazy val reverseScoreOrdering = scoreOrdering.reverse

  private[this] val queue: InternalRowPriorityQueue = {
    new InternalRowPriorityQueue(Math.abs(k))(new Ordering[QueueType] {
      override def compare(x: QueueType, y: QueueType): Int =
        scoreOrdering.compare(x._1, y._1)
    })
  }

  lazy private[this] val scoreProjection: UnsafeProjection =
    UnsafeProjection.create(scoreExpression :: Nil, output)

  // The grouping key of the current partition
  private[this] var currentGroupingKey: UnsafeRow = _

  @transient private[this] lazy val boundCondition = if (condition.isDefined) {
    newPredicate(condition.get, streamedPlan.output ++ buildPlan.output)
  } else {
    (r: InternalRow) => true
  }

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  private def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    // val buildDataSize = longMetric("buildDataSize")
    // val buildTime = longMetric("buildTime")
    // val start = System.nanoTime()
    val context = TaskContext.get()
    val relation = HashedRelation(iter, buildKeys, taskMemoryManager = context.taskMemoryManager())
    // buildTime += (System.nanoTime() - start) / 1000000
    // buildDataSize += relation.estimatedSize
    // This relation is usually used until the end of task.
    context.addTaskCompletionListener(_ => relation.close())
    relation
  }

  def close(): Unit = {
    val tc = TaskContext.get()
    logInfo(s"StageID:${tc.stageId} TaskID:${tc.taskAttemptId()} #Processed:${_numProcessedRows}" +
      s" #inputs: ${_numInputRows} #outputs:${_numOutputRows}" +
      " Elapsed:" + ((System.nanoTime() - startTime) / 1000000))
  }

  protected def topKInnerJoin(
      streamedIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
    val joinRow = new JoinedRow
    val joinKeysProj = streamSideKeyGenerator()
    val joinedIter = streamedIter.flatMap { srow =>
      _numInputRows += 1
      joinRow.withLeft(srow)
      val joinKeys = joinKeysProj(srow) // `joinKeys` is also a grouping key
      val matches = hashedRelation.get(joinKeys)
      if (matches != null) {
        /**
        matches.map(joinRow.withRight(_)).filter(boundCondition).flatMap { resultRow =>
          val ret = if (currentGroupingKey != joinKeys) {
            val topKRows = queue.iterator.toSeq.sortBy(_._1)(reverseScoreOrdering).map(_._2)
            currentGroupingKey = joinKeys.copy()
            queue.clear()
            topKRows
          } else {
            Seq.empty
          }
          queue += Tuple2(scoreProjection(resultRow).get(0, scoreType), resultRow.copy())
          ret
        }
         */
        matches.map(joinRow.withRight(_)).filter(boundCondition).foreach { resultRow =>
          _numProcessedRows += 1
          queue += Tuple2(scoreProjection(resultRow).get(0, scoreType), resultRow)
        }
        val iter = queue.iterator.toSeq.sortBy(_._1)(reverseScoreOrdering).map(_._2)
        queue.clear
        iter
      } else {
        Seq.empty
      }
    }
    val resultProj = createResultProjection
    CompletionIterator[InternalRow, Iterator[InternalRow]](
      (joinedIter ++ queue.iterator.toSeq.sortBy(_._1)(reverseScoreOrdering)
          .map(_._2)).map { r =>
        _numOutputRows += 1
        resultProj(r)
      },
      close())
  }

  override protected def doExecute(): RDD[InternalRow] = {
    startTime = System.nanoTime()
    // val numOutputRows = longMetric("numOutputRows")
    streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter)
      // topKInnerJoin(streamedIter, hashed, numOutputRows)
      topKInnerJoin(streamIter, hashed, null)
    }
  }
}

private[sql] object TopKShuffledHashJoinExec {

    def createWithExchangeExec(
      k: Int,
      scoreExpr: NamedExpression,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan): SparkPlan = {
    val op = new TopKShuffledHashJoinExec(
      k, scoreExpr, leftKeys, rightKeys, BuildRight, condition, left, right)
    val requiredChildDistributions = op.requiredChildDistribution
    var children: Seq[SparkPlan] = op.children
    assert(requiredChildDistributions.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements:
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) => child
      case _ => throw new SparkException("Unsupported input distribution")
    }
    val newOp = op.withNewChildren(children)
    ProjectExec(newOp.output :+ scoreExpr, newOp)
  }
}
