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

import scala.reflect.ClassTag
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow

/**
 * A fast hash map implementation for nullable keys. This hash map supports insertions and updates,
 * but not deletions. This map is about 5X faster than java.util.HashMap, while using much less
 * space overhead.
 *
 * Under the hood, it uses our MultiColumnOpenHashSet implementation.
 */
private[spark] class MultiColumnOpenHashMap[@specialized(Long, Int, Double) V: ClassTag](
    val columns: Array[Int],
    val types: Array[DataType],
    val numColumns: Int,
    val initialCapacity: Int,
    val loadFactor: Double) extends Iterable[(Row, V)] with Serializable {

  def this(columns: Array[Int], types: Array[DataType], initialCapacity: Int) =
    this(columns, types, columns.length, initialCapacity, 0.7)

  def this(columns: Array[Int], types: Array[DataType]) =
    this(columns, types, 64)

  protected var _keySet = new MultiColumnOpenHashSet(columns, types,
    numColumns, initialCapacity, loadFactor)

  // Init in constructor (instead of in declaration) to work around a Scala compiler specialization
  // bug that would generate two arrays (one for Object and one for specialized T).
  private var _values: Array[V] = _
  _values = new Array[V](_keySet.capacity)

  @transient private var _oldValues: Array[V] = null

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  private var haveNullValue = false
  private var nullValue: V = null.asInstanceOf[V]

  override def size: Int = if (haveNullValue) _keySet.size + 1 else _keySet.size

  /** Tests whether this map contains a binding for a key. */
  def contains(k: Row): Boolean = {
    if (k != null) {
      _keySet.getPos(k) != MultiColumnOpenHashSet.INVALID_POS
    } else {
      haveNullValue
    }
  }

  /** Get the value for a given key */
  def apply(k: Row): V = {
    if (k != null) {
      val pos = _keySet.getPos(k)
      if (pos >= 0) {
        _values(pos)
      } else {
        null.asInstanceOf[V]
      }
    } else {
      nullValue
    }
  }

  /** Set the value for a key */
  def update(k: Row, v: V) {
    if (k != null) {
      val pos = _keySet.addWithoutResize(k) & MultiColumnOpenHashSet.POSITION_MASK
      _values(pos) = v
      _keySet.rehashIfNeeded(k, grow, move)
      _oldValues = null
    } else {
      haveNullValue = true
      nullValue = v
    }
  }

  /**
   * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
   * set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeValue(k: Row, defaultValue: => V, mergeValue: (V) => V): V = {
    if (k != null) {
      val pos = _keySet.addWithoutResize(k)
      if ((pos & MultiColumnOpenHashSet.NONEXISTENCE_MASK) != 0) {
        val newValue = defaultValue
        _values(pos & MultiColumnOpenHashSet.POSITION_MASK) = newValue
        _keySet.rehashIfNeeded(k, grow, move)
        newValue
      } else {
        _values(pos) = mergeValue(_values(pos))
        _values(pos)
      }
    } else {
      if (haveNullValue) {
        nullValue = mergeValue(nullValue)
      } else {
        haveNullValue = true
        nullValue = defaultValue
      }
      nullValue
    }
  }

  /**
   * If the key doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise,set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeKeyValue(k: Row, keyValue: () => (Row, V),
                     mergeValue: (V) => Unit): V = {
    if (k != null) {
      val pos = _keySet.addWithoutResize(k)
      if ((pos & MultiColumnOpenHashSet.NONEXISTENCE_MASK) == 0) {
        mergeValue(_values(pos))
        _values(pos)
      } else {
        val posn = pos & MultiColumnOpenHashSet.POSITION_MASK
        val (newKey, newValue) = keyValue()
        _keySet.setValue(posn, newKey)
        _values(posn) = newValue
        _keySet.rehashIfNeeded(newKey, grow, move)
        newValue
      }
    } else {
      if (haveNullValue) {
        nullValue
      } else {
        haveNullValue = true
        val (newKey, newValue) = keyValue()
        nullValue = newValue
      }
      nullValue
    }
  }

  override def iterator: Iterator[(Row, V)] = new Iterator[(Row, V)] {
    var pos = -1
    var currentKey: SpecificMutableRow = _keySet.newEmptyValueAsRow()
    var nextPair: (Row, V) = computeNextPair()

    /** Get the next value we should return from next(), or null if we're finished iterating */
    def computeNextPair(): (Row, V) = {
      if (pos == -1) { // Treat position -1 as looking at the null value
        if (haveNullValue) {
          pos += 1
          return (null, nullValue)
        }
        pos += 1
      }
      pos = _keySet.nextPos(pos)
      if (pos >= 0) {
        _keySet.getValueAsRow(pos, currentKey)
        val ret = (currentKey, _values(pos))
        pos += 1
        ret
      } else {
        null
      }
    }

    def hasNext: Boolean = nextPair != null

    def next(): (Row, V) = {
      val pair = nextPair
      nextPair = computeNextPair()
      pair
    }
  }

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).
  // They also should have been val's. We use var's because there is a Scala compiler bug that
  // would throw illegal access error at runtime if they are declared as val's.
  protected var grow = (newCapacity: Int) => {
    _oldValues = _values
    _values = new Array[V](newCapacity)
  }

  protected var move = (oldPos: Int, newPos: Int) => {
    _values(newPos) = _oldValues(oldPos)
  }
}
