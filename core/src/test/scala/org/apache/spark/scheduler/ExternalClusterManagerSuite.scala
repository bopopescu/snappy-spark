/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.spark.scheduler

import org.scalatest.BeforeAndAfter
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.{SparkContext, SparkFunSuite, SparkConf}


class ExternalClusterManagerSuite extends SparkFunSuite
with BeforeAndAfter {

  val mycm = new ExternalClusterManager {

    def canCreate(masterURL: String): Boolean = masterURL == "myclusterManager"

    def createTaskScheduler(sc: SparkContext): TaskScheduler = new TaskSchedulerImpl(sc,
      1, isLocal = true)

    def createSchedulerBackend(sc: SparkContext, scheduler: TaskScheduler): SchedulerBackend =
      new LocalBackend(sc.getConf, scheduler.asInstanceOf[TaskSchedulerImpl], 2)

    def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit =
      scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)

  }
  before {
    SparkContext.registerClusterManager(mycm)
  }
  after {
    SparkContext.unregisterClusterManager(mycm)
  }

  test("launch of backend and scheduler") {
    val conf = new SparkConf().setMaster("myclusterManager").setAppName("testcm")
    val sc = new SparkContext(conf)
    // check if the scheduler components are created
    assert(sc.schedulerBackend.isInstanceOf[LocalBackend])
    assert(sc.taskScheduler.isInstanceOf[TaskSchedulerImpl])
    // check if the intialization has happened.
    assert(sc.taskScheduler.asInstanceOf[TaskSchedulerImpl].backend != null)
  }
}
