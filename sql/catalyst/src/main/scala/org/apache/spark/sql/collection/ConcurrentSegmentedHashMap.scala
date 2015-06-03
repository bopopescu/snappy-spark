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

package org.apache.spark.sql.collection

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

private[sql] class ConcurrentSegmentedHashMap[K, V, M <: SegmentMap[K, V] : ClassTag]
(
  private val initialSize: Int,
  val loadFactor: Double,
  val concurrency: Int,
  val segmentCreator: (Int, Double) => M,
  val hasher: K => Int) extends Serializable {

  /**
   * A default constructor creates a concurrent hash map with initial size `32`
   * and concurrency `16`.
   */
  def this(segmentCreator: (Int, Double) => M, hasher: K => Int) =
    this(32, SegmentMap.DEFAULT_LOAD_FACTOR, 16, segmentCreator, hasher)

  require(initialSize > 0,
    s"ConcurrentSegmentedHashMap: unexpected initialSize=$initialSize")
  require(loadFactor > 0.0 && loadFactor < 1.0,
    s"ConcurrentSegmentedHashMap: unexpected loadFactor=$loadFactor")
  require(concurrency > 0,
    s"ConcurrentSegmentedHashMap: unexpected concurrency=$concurrency")
  require(segmentCreator != null,
    "ConcurrentSegmentedHashMap: null segmentCreator")

  private val _segments: Array[M] = {
    val nsegs = math.min(concurrency, 1 << 16)
    val segs = new Array[M](nsegs)
    // calculate the initial capacity of each segment
    val segCapacity = math.max(2, SegmentMap.nextPowerOf2(initialSize / nsegs))
    segs.indices.foreach(segs(_) = segmentCreator(segCapacity, loadFactor))
    segs
  }
  private val _size = new AtomicLong(0)

  private val (_segmentShift, _segmentMask) = {
    var sshift = 0
    var ssize = 1
    val concurrency = _segments.length
    if (concurrency > 1) {
      while (ssize < concurrency) {
        sshift += 1
        ssize <<= 1
      }
    }
    (32 - sshift, ssize - 1)
  }

  private final def segmentFor(hash: Int): M = {
    _segments((hash >>> _segmentShift) & _segmentMask)
  }

  final def contains(k: K): Boolean = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.readLock
    lock.lock()
    try {
      seg.contains(k, hash)
    } finally {
      lock.unlock()
    }
  }

  final def apply(k: K): V = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.readLock
    lock.lock()
    try {
      seg(k, hash)
    } finally {
      lock.unlock()
    }
  }

  final def get(k: K): Option[V] = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.readLock
    lock.lock()
    try {
      Option(seg(k, hash))
    } finally {
      lock.unlock()
    }
  }

  final def update(k: K, v: V): Boolean = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.writeLock
    var added = false
    lock.lock()
    try {
      added = seg.update(k, hash, v)
    } finally {
      lock.unlock()
    }
    if (added) {
      _size.incrementAndGet()
      true
    } else false
  }

  final def changeValue(k: K, change: ChangeValue[K, V]): Unit = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.writeLock
    var added = false
    lock.lock()
    try {
      added = seg.changeValue(k, hash, change)
    } finally {
      lock.unlock()
    }
    if (added) _size.incrementAndGet()
  }

  final def bulkChangeValues(ks: TraversableOnce[K],
                             change: ChangeValue[K, V]): Unit = {
    val segs = this._segments
    val segShift = _segmentShift
    val segMask = _segmentMask
    val hasher = this.hasher

    // first group keys by segments
    val nsegs = segs.length
    val groupedKeys = new Array[mutable.ArrayBuffer[K]](nsegs)
    val groupedHashes = new Array[mutable.ArrayBuilder.ofInt](nsegs)
    ks.foreach { k =>
      val hash = if (hasher != null) hasher(k) else k.##
      val segIndex = (hash >>> segShift) & segMask
      val buffer = groupedKeys(segIndex)
      if (buffer != null) {
        buffer += k
        groupedHashes(segIndex) += hash
      } else {
        val newBuffer = new mutable.ArrayBuffer[K](4)
        val newHashBuffer = new mutable.ArrayBuilder.ofInt()
        newHashBuffer.sizeHint(4)
        newBuffer += k
        newHashBuffer += hash
        groupedKeys(segIndex) = newBuffer
        groupedHashes(segIndex) = newHashBuffer
      }
    }

    // now lock segments one by one and then apply changes for all keys
    // of the locked segment
    var added = 0
    // shuffle the indexes to minimize segment thread contention
    Random.shuffle[Int, IndexedSeq](0 until nsegs).foreach { i =>
      val keys = groupedKeys(i)
      if (keys != null) {
        val hashes = groupedHashes(i).result()
        val nhashes = hashes.length
        val seg = segs(i)
        val lock = seg.writeLock
        lock.lock()
        try {
          var idx = 0
          do {
            if (seg.changeValue(keys(idx), hashes(idx), change)) {
              added += 1
            }
            idx += 1
          } while (idx < nhashes)
        } finally {
          lock.unlock()
        }
        // invoke the segmentEnd method outside of the segment lock
        change.segmentEnd(seg)
      }
    }
    if (added > 0) _size.addAndGet(added)
  }

  def foldRead[U](init: U)(f: (K, V, U) => U): U = {
    _segments.foldLeft(init) { (v, seg) =>
      SegmentMap.lock(seg.readLock()) {
        seg.fold(v)(f)
      }
    }
  }

  def foldWrite[U](init: U)(f: (K, V, U) => U): U = {
    _segments.foldLeft(init) { (v, seg) =>
      SegmentMap.lock(seg.writeLock()) {
        seg.fold(v)(f)
      }
    }
  }

  def readLock[U](f: (Seq[M]) => U): U = {
    val segments = wrapRefArray(_segments)
    segments.foreach(_.readLock.lock())
    try {
      f(segments)
    } finally {
      segments.foreach(_.readLock.unlock())
    }
  }

  def writeLock[U](f: (Seq[M]) => U): U = {
    val segments = wrapRefArray(_segments)
    segments.foreach(_.writeLock.lock())
    try {
      f(segments)
    } finally {
      segments.foreach(_.writeLock.unlock())
    }
  }

  final def size = _size.get

  final def isEmpty = _size.get == 0

  def toSeq: Seq[(K, V)] = {
    val size = this.size
    if (size <= Int.MaxValue) {
      val buffer = new mutable.ArrayBuffer[(K, V)](size.toInt)
      foldRead[Unit](){ (k, v, u) => buffer += ((k, v)) }
      buffer
    }
    else {
      throw new IllegalStateException(s"ConcurrentSegmentedHashMap: size=$size" +
        " is greater than maximum integer so cannot be converted to a flat Seq")
    }
  }

  def toValues: Seq[V] = {
    val size = this.size
    if (size <= Int.MaxValue) {
      val buffer = new mutable.ArrayBuffer[V](size.toInt)
      foldRead[Unit](){ (k, v, u) => buffer += v }
      buffer
    }
    else {
      throw new IllegalStateException(s"ConcurrentSegmentedHashMap: size=$size" +
        " is greater than maximum integer so cannot be converted to a flat Seq")
    }
  }
}