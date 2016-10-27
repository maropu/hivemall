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

import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.util.KnownSizeEstimation

case class TopKJoinExec(
    k: Int,
    scoreExpression: Expression,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    condition: Option[Expression],
    broadcastRelation1: Broadcast[HashedRelation],
    reusedBroadcast: Boolean,
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with HashJoin {
  type QueueType = (AnyRef, InternalRow)

  require(k != 0, "`k` must not have 0")

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

  private[this] val queue: BoundedPriorityQueue[QueueType] = {
    new BoundedPriorityQueue(Math.abs(k))(new Ordering[QueueType] {
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

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeys)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // val numOutputRows = longMetric("numOutputRows")

    val broadcastRelation = if (!reusedBroadcast) {
      buildPlan.executeBroadcast[HashedRelation]()
    } else {
      broadcastRelation1
    }
    streamedPlan.execute().mapPartitions { streamedIter =>
      val hashed = broadcastRelation.value.asReadOnlyCopy()
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashed.estimatedSize)
      // topKInnerJoin(streamedIter, hashed, numOutputRows)
      topKInnerJoin(streamedIter, hashed, null)
    }
  }

  protected def topKInnerJoin(
      streamedIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
    val joinRow = new JoinedRow
    val joinKeysProj = streamSideKeyGenerator()
    val joinedIter = streamedIter.flatMap { srow =>
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
          queue += Tuple2(scoreProjection(resultRow).get(0, scoreType), resultRow.copy())
        }
        val iter = queue.iterator.toSeq.sortBy(_._1)(reverseScoreOrdering).map(_._2)
        queue.clear
        iter
      } else {
        Seq.empty
      }
    }
    val resultProj = createResultProjection
    (joinedIter ++ queue.iterator.toSeq.sortBy(_._1)(reverseScoreOrdering).map(_._2)).map { r =>
      // numOutputRows += 1
      resultProj(r)
    }
  }
}

private[sql] object TopKJoinExec {

  def createWithBroadcastExec(
      k: Int,
      scoreExpr: NamedExpression,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      broadcastRelation: Broadcast[KnownSizeEstimation],
      reusedBroadcast: Boolean,
      left: SparkPlan,
      right: SparkPlan): SparkPlan = {
    val op = TopKJoinExec(k, scoreExpr, leftKeys, rightKeys, BuildRight, condition,
      broadcastRelation.asInstanceOf[Broadcast[HashedRelation]], reusedBroadcast, left, right)
    val requiredChildDistributions = op.requiredChildDistribution
    var children: Seq[SparkPlan] = op.children
    assert(requiredChildDistributions.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements:
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
    }
    val newOp = op.withNewChildren(children)
    ProjectExec(newOp.output :+ scoreExpr, newOp)
  }

  def broadcastRelation(df: DataFrame, keys: Seq[Expression])(implicit _sc: SparkContext)
    : Broadcast[HashedRelation] = {
    val rkeys = rewriteKeyExpr(keys)
      .map(BindReferences.bindReference(_, df.queryExecution.sparkPlan.output))

    try {
      val beforeCollect = System.nanoTime()
      // Note that we use .executeCollect() because we don't want to convert data to Scala types
      val input: Array[InternalRow] = df.queryExecution.executedPlan(0).executeCollect()

      if (input.length >= 512000000) {
        throw new SparkException(
          s"Cannot broadcast the table with more than 512 millions rows: ${input.length} rows")
      }
      val beforeBuild = System.nanoTime()
      println(s"collectTime: ${(beforeBuild - beforeCollect) / 1000000}")
      val dataSize = input.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
      println(s"dataSize: ${dataSize}")
      if (dataSize >= (8L << 30)) {
        throw new SparkException(
          s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
      }

      // Construct and broadcast the relation.
      val relation = HashedRelationBroadcastMode(rkeys).transform(input)
      val beforeBroadcast = System.nanoTime()
      println(s"buildTime: ${(beforeBroadcast - beforeBuild) / 1000000}")

      val broadcasted = _sc.broadcast(relation)
      println(s"broadcastTime:${(System.nanoTime() - beforeBroadcast) / 1000000}")

      broadcasted
    } catch {
      case oe: OutOfMemoryError =>
        throw new OutOfMemoryError(s"Not enough memory to build and broadcast the table to " +
          s"all worker nodes. As a workaround, you can either disable broadcast by setting " +
          s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark driver " +
          s"memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value")
          .initCause(oe.getCause)
    }
  }

  /**
   * Try to rewrite the key as LongType so we can use getLong(), if they key can fit with a long.
   *
   * If not, returns the original expressions.
   */
  private[joins] def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    assert(keys.nonEmpty)
    // TODO: support BooleanType, DateType and TimestampType
    if (keys.exists(!_.dataType.isInstanceOf[IntegralType])
      || keys.map(_.dataType.defaultSize).sum > 8) {
      return keys
    }

    var keyExpr: Expression = if (keys.head.dataType != LongType) {
      Cast(keys.head, LongType)
    } else {
      keys.head
    }
    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(Cast(e, LongType), Literal((1L << bits) - 1)))
    }
    keyExpr :: Nil
  }
}
