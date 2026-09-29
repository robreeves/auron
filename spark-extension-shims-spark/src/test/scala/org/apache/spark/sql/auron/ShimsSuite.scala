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
package org.apache.spark.sql.auron

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.physical.{IdentityBroadcastMode, SinglePartition}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, LocalTableScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.auron.sparkver

class ShimsSuite extends SparkFunSuite with SharedSparkSession {
  private val shims = new ShimsImpl

  @sparkver("3.0 / 3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  private def scan: SparkPlan = LocalTableScanExec(Seq.empty, Seq.empty)

  @sparkver("4.0 / 4.1 / 4.2")
  private def scan: SparkPlan = LocalTableScanExec(Seq.empty, Seq.empty, None)

  for (native <- Seq(false, true)) {
    test(s"recognize direct shuffle query stage input (native=$native)") {
      val exchange = if (native) {
        NativeShuffleExchangeExec(SinglePartition, scan)
      } else {
        ShuffleExchangeExec(SinglePartition, scan)
      }
      assert(shims.isShuffleQueryStageInput(shuffleStage(exchange)))
    }

    test(s"recognize adaptive shuffle query stage input (native=$native)") {
      val exchange = if (native) {
        NativeShuffleExchangeExec(SinglePartition, scan)
      } else {
        ShuffleExchangeExec(SinglePartition, scan)
      }
      assert(shims.isShuffleQueryStageInput(shuffleRead(shuffleStage(exchange))))
    }
  }

  test("exclude broadcast query stages") {
    val stage = broadcastStage(BroadcastExchangeExec(IdentityBroadcastMode, scan))
    assert(!shims.isShuffleQueryStageInput(stage))
    assert(!shims.isShuffleQueryStageInput(shuffleRead(stage)))
  }

  test("exclude ordinary operators and shuffle reads without an immediate shuffle stage") {
    val exchange = ShuffleExchangeExec(SinglePartition, scan)
    val project = ProjectExec(Seq.empty, shuffleStage(exchange))
    assert(!shims.isShuffleQueryStageInput(scan))
    assert(!shims.isShuffleQueryStageInput(exchange))
    assert(!shims.isShuffleQueryStageInput(project))
    assert(!shims.isShuffleQueryStageInput(shuffleRead(exchange)))
    assert(!shims.isShuffleQueryStageInput(shuffleRead(project)))
  }

  @sparkver("3.0 / 3.1")
  private def shuffleStage(plan: SparkPlan): SparkPlan = ShuffleQueryStageExec(0, plan)

  @sparkver("3.2 / 3.3 / 3.4 / 3.5 / 4.0 / 4.1 / 4.2")
  private def shuffleStage(plan: SparkPlan): SparkPlan =
    ShuffleQueryStageExec(0, plan, plan.canonicalized)

  @sparkver("3.0 / 3.1")
  private def broadcastStage(plan: SparkPlan): SparkPlan = BroadcastQueryStageExec(0, plan)

  @sparkver("3.2 / 3.3 / 3.4 / 3.5 / 4.0 / 4.1 / 4.2")
  private def broadcastStage(plan: SparkPlan): SparkPlan =
    BroadcastQueryStageExec(0, plan, plan.canonicalized)

  @sparkver("3.0")
  private def shuffleRead(plan: SparkPlan): SparkPlan = {
    import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
    CustomShuffleReaderExec(plan, Seq(CoalescedPartitionSpec(0, 1)), "test")
  }

  @sparkver("3.1")
  private def shuffleRead(plan: SparkPlan): SparkPlan = {
    import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
    CustomShuffleReaderExec(plan, Seq(CoalescedPartitionSpec(0, 1)))
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5 / 4.0 / 4.1 / 4.2")
  private def shuffleRead(plan: SparkPlan): SparkPlan = {
    import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
    AQEShuffleReadExec(plan, Seq(CoalescedPartitionSpec(0, 1)))
  }
}
