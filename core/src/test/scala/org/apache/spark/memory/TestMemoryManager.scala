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

package org.apache.spark.memory

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockStatus, BlockId}

class TestMemoryManager(conf: SparkConf) extends MemoryManager(conf, numCores = 1) {
  private[memory] override def doAcquireExecutionMemory(
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Long = synchronized {
    if (oomOnce) {
      oomOnce = false
      0
    } else if (available >= numBytes) {
      _executionMemoryUsed += numBytes // To suppress warnings when freeing unallocated memory
      available -= numBytes
      numBytes
    } else {
      _executionMemoryUsed += available
      val grant = available
      available = 0
      grant
    }
  }
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = true
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = true
  override def releaseExecutionMemory(numBytes: Long): Unit = {
    available += numBytes
    _executionMemoryUsed -= numBytes
  }
  override def releaseStorageMemory(numBytes: Long): Unit = {}
  override def maxExecutionMemory: Long = Long.MaxValue
  override def maxStorageMemory: Long = Long.MaxValue

  private var oomOnce = false
  private var available = Long.MaxValue

  def markExecutionAsOutOfMemoryOnce(): Unit = {
    oomOnce = true
  }

  def limit(avail: Long): Unit = {
    available = avail
  }

}
