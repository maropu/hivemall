/**
 * An initialization script for DataFrame use
 */

import org.apache.spark.ml.feature.HmLabeledPoint
import org.apache.spark.ml.feature.HmFeature
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils
// Needed for implicit conversions
import org.apache.spark.sql.hive.HivemallUtils._
import sqlContext.implicits._

val ft2vec = HivemallUtils.funcVectorizer()

