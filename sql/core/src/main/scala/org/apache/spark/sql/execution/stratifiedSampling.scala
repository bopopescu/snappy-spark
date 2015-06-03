package org.apache.spark.sql.execution

import java.util.Random
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.Logging
import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.{Attribute, EmptyRow}
import org.apache.spark.sql.collection._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.util.random.RandomSampler

import scala.collection.mutable
import scala.util.Sorting

/**
 * Perform stratified sampling given a Query-Column-Set (QCS). This variant
 * can also use a fixed fraction to be sampled instead of fixed number of
 * total samples since it is eventually designed to be used with streaming data.
 */
case class StratifiedSample(options: Map[String, Any], tableSchema: StructType,
                            child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  protected override def doExecute(): RDD[Row] = {
    new PartitionwiseSampledRDD[Row, Row](child.execute(),
      StratifiedSampler(options, tableSchema, cached = false), true)
  }
}

object StratifiedSampler {

  private val globalMap = new mutable.HashMap[String, StratifiedSampler]
  private val mapLock = new ReentrantReadWriteLock

  implicit class StringExtensions(val s: String) extends AnyVal {
    def ci = new {
      def unapply(other: String) = s.equalsIgnoreCase(other)
    }
  }

  def fillArray[T](a: Array[T], v: T, start: Int, endP1: Int) = {
    var index = start
    while (index < endP1) {
      a(index) = v
      index += 1
    }
  }

  def columnIndex(col: String, cols: Array[String]) = {
    val colT = col.trim
    val colIndex = cols.indices.foldLeft(-1) { (idx, i) =>
      if (colT.equalsIgnoreCase(cols(i))) i else idx
    }
    if (colIndex >= 0) colIndex
    else throw new AnalysisException(
      s"""StratifiedSampler: Cannot resolve column name "$col" among
            (${cols.mkString(", ")})""")
  }

  def qcsOf(qa: Array[String], cols: Array[String]): Array[Int] = {
    val colIndexes = qa.map {
      columnIndex(_, cols)
    }
    Sorting.quickSort(colIndexes)
    colIndexes
  }

  def qcsOf(qa: Array[String], schema: StructType): Array[Int] =
    qcsOf(qa, schema.fieldNames)

  def apply(options: Map[String, Any],
            schema: StructType, cached: Boolean): StratifiedSampler = {
    val nameTest = "name".ci
    val qcsTest = "qcs".ci
    val fracTest = "fraction".ci
    val reservoirSizeTest = "strataReservoirSize".ci
    val timeSeriesColumnTest = "timeSeriesColumn".ci
    val timeIntervalTest = "timeInterval".ci

    val timeIntervalSpec = "([0-9]+)(s|m|h)".r
    val cols = schema.fieldNames

    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (qcs, fraction, strataReservoirSize) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (qcs, nm, fraction, strataSize, tsCol, timeInterval) = options.foldLeft(
      Array.emptyIntArray, "", 0.0, 0, -1, 0) {
      case ((qs, n, fr, sz, ts, ti), (opt, optV)) =>
        opt match {
          case qcsTest() => optV match {
            case qi: Array[Int] => (qi, n, fr, sz, ts, ti)
            case q: String => (qcsOf(q.split(","), cols), n, fr, sz, ts, ti)
            case _ => throw new AnalysisException(
              s"""StratifiedSampler: Cannot parse 'qcs'="$optV" among
                  (${cols.mkString(", ")})""")
          }
          case nameTest() => (qs, optV.toString, fr, sz, ts, ti)
          case fracTest() => optV match {
            case fd: Double => (qs, n, fd, sz, ts, ti)
            case fs: String => (qs, n, fs.toDouble, sz, ts, ti)
            case ff: Float => (qs, n, ff.toDouble, sz, ts, ti)
            case fi: Int => (qs, n, fi.toDouble, sz, ts, ti)
            case fl: Long => (qs, n, fl.toDouble, sz, ts, ti)
            case _ => throw new AnalysisException(
              s"StratifiedSampler: Cannot parse double 'fraction'=$optV")
          }
          case reservoirSizeTest() => optV match {
            case si: Int => (qs, n, fr, si, ts, ti)
            case ss: String => (qs, n, fr, ss.toInt, ts, ti)
            case sl: Long => (qs, n, fr, sl.toInt, ts, ti)
            case _ => throw new AnalysisException(
              s"StratifiedSampler: Cannot parse int 'strataReservoirSize'=$optV")
          }
          case timeSeriesColumnTest() => optV match {
            case tss: String => (qs, n, fr, sz, columnIndex(tss, cols), ti)
            case tsi: Int => (qs, n, fr, sz, tsi, ti)
            case _ => throw new AnalysisException(
              s"StratifiedSampler: Cannot parse 'timeSeriesColumn'=$optV")
          }
          case timeIntervalTest() => optV match {
            case tii: Int => (qs, n, fr, sz, ts, tii)
            case til: Long => (qs, n, fr, sz, ts, til.toInt)
            case tis: String => tis match {
              case timeIntervalSpec(interval, unit) =>
                unit match {
                  case "s" => (qs, n, fr, sz, ts, interval.toInt)
                  case "m" => (qs, n, fr, sz, ts, interval.toInt * 60)
                  case "h" => (qs, n, fr, sz, ts, interval.toInt * 3600)
                  case _ => throw new AssertionError(
                    s"unexpected regex match 'unit'=$unit")
                }
              case _ => throw new AnalysisException(
                s"StratifiedSampler: Cannot parse 'timeInterval'=$tis")
            }
            case _ => throw new AnalysisException(
              s"StratifiedSampler: Cannot parse 'timeInterval'=$optV")
          }
          case _ => throw new AnalysisException(
            s"""StratifiedSampler: Unknown option "$opt"""")
        }
    }

    if (cached && nm.nonEmpty) {
      lookupOrAdd(qcs, nm, fraction, strataSize, tsCol, timeInterval, schema)
    }
    else {
      newSampler(qcs, nm, fraction, strataSize,
        tsCol, timeInterval, schema, cached = false)
    }
  }

  private[sql] def lookupOrAdd(qcs: Array[Int], name: String,
                               fraction: Double, strataSize: Int,
                               tsCol: Int, timeInterval: Int,
                               schema: StructType): StratifiedSampler = {
    // not using getOrElse in one shot to allow taking only read lock
    // for the common case, then release it and take write lock if new
    // sampler has to be added
    SegmentMap.lock(mapLock.readLock) {
      globalMap.get(name)
    } match {
      case Some(sampler) => sampler
      case None =>
        val sampler = newSampler(qcs, name, fraction, strataSize,
          tsCol, timeInterval, schema, cached = true)
        // insert into global map but double-check after write lock
        SegmentMap.lock(mapLock.writeLock) {
          globalMap.getOrElse(name, {
            globalMap(name) = sampler
            sampler
          })
        }
    }
  }

  def removeSampler(name: String): Option[StratifiedSampler] =
    SegmentMap.lock(mapLock.writeLock) {
      globalMap.remove(name)
    }

  private def newSampler(qcs: Array[Int], name: String, fraction: Double,
                         strataSize: Int, tsCol: Int, timeInterval: Int,
                         schema: StructType,
                         cached: Boolean): StratifiedSampler = {
    if (qcs.isEmpty)
      throw new AnalysisException("StratifiedSampler: QCS is empty")
    else if (tsCol >= 0 && timeInterval <= 0)
      throw new AnalysisException("StratifiedSampler: no timeInterval for " +
        "timeSeriesColumn=" + schema(tsCol).name)
    else if (fraction > 0.0)
      new StratifiedSamplerCached(qcs, name, schema, cached, new AtomicInteger(
        math.max(strataSize, 50)), fraction, tsCol, timeInterval)
    else if (strataSize > 0)
      new StratifiedSamplerReservoir(qcs, name, schema, cached, strataSize)
    else throw new AnalysisException("StratifiedSampler: " +
      s"'fraction'=$fraction 'strataReservoirSize'=$strataSize")
  }
}

abstract class StratifiedSampler(val qcs: Array[Int], val name: String,
                                 val schema: StructType, val cached: Boolean)
  extends RandomSampler[Row, Row]
  with ChangeValue[Row, StrataReservoir] with Logging {

  self =>

  type ReservoirSegment = MultiColumnOpenHashMap[StrataReservoir]

  protected final class ProcessRows[U](val process: (U, Row) => U,
                                       val endBatch: U => U, var aggregate: U)
    extends ChangeValue[Row, StrataReservoir] {

    override def defaultValue(row: Row): StrataReservoir = {
      self.defaultValue(row)
    }

    override def mergeValue(row: Row, sr: StrataReservoir): StrataReservoir = {
      self.mergeValue(row, sr)
    }

    override def segmentEnd(segment: SegmentMap[Row, StrataReservoir]): Unit = {
      aggregate = self.segmentEnd(segment, aggregate, process, endBatch)
    }
  }

  /**
   * Map of each strata key (i.e. a unique combination of values of columns
   * in qcs) to related metadata and reservoir
   */
  protected final val stratas = {
    val numColumns = qcs.length
    val types: Array[DataType] = new Array[DataType](numColumns)
    0 until numColumns foreach { i =>
      types(i) = schema(qcs(i)).dataType
    }
    val columnHandler = MultiColumnOpenHashSet.newColumnHandler(qcs,
      types, numColumns)
    val hasher = { row: Row => columnHandler.hash(row) }
    new ConcurrentSegmentedHashMap[Row, StrataReservoir, ReservoirSegment](
      (initialCapacity, loadFactor) => new ReservoirSegment(qcs, types,
        numColumns, initialCapacity, loadFactor), hasher)
  }

  /**
   * Random number generator for sampling.
   */
  protected final val rng = new Random()

  /** Keeps track of the maximum number of samples in a strata seen so far */
  protected val nmaxSamples = new AtomicInteger

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  protected def strataReservoirSize: Int

  def append[U](rows: Iterator[Row], init: U, process: (U, Row) => U,
                endBatch: U => U): U

  override def segmentEnd(segment: SegmentMap[Row, StrataReservoir]): Unit = {}

  def segmentEnd[U](segment: SegmentMap[Row, StrataReservoir], init: U,
                    process: (U, Row) => U, endBatch: U => U): U = init

  /** return a copy of the StratifiedSampler object */
  override def clone: StratifiedSampler =
    throw new NotImplementedError("clone() is not implemented.")

  protected final def foldSegment[U](prevReservoirSize: Int, fullReset: Boolean,
                                     process: (U, Row) => U)
                                    (u: U, seg: ReservoirSegment): U = {
    seg.fold(u)(foldReservoir(prevReservoirSize, fullReset, process))
  }

  protected final def foldReservoir[U](prevReservoirSize: Int, fullReset: Boolean,
                                       process: (U, Row) => U)
                                      (row: Row, sr: StrataReservoir, u: U): U = {
    // imperative code segment below for best efficiency
    var v = u
    val reservoir = sr.reservoir
    val nsamples = sr.reservoirSize
    var index = 0
    while (index < nsamples) {
      v = process(v, reservoir(index))
      index += 1
    }
    // reset transient data
    sr.reset(prevReservoirSize, strataReservoirSize, fullReset)
    v
  }
}

// TODO: optimize by having metadata as multiple columns like key;
// TODO: add a good sparse array implementation

/**
 * For each strata (i.e. a unique set of values for QCS), keep a set of
 * meta-data including number of samples collected, total number of rows
 * in the strata seen so far, the QCS key, reservoir of samples etc.
 */
final class StrataReservoir(var totalSamples: Int, var batchTotalSize: Int,
                            var weightage: Double, var reservoir: Array[Row],
                            var reservoirSize: Int, var prevShortFall: Int) {

  self =>

  def iterator(prevReservoirSize: Int, newReservoirSize: Int,
               fullReset: Boolean): Iterator[Row] = {
    new Iterator[Row] {

      val reservoir = self.reservoir
      val nsamples = self.reservoirSize
      var pos = 0

      override def hasNext: Boolean = {
        if (pos < nsamples) {
          true
        } else {
          self.reset(prevReservoirSize, newReservoirSize, fullReset)
          false
        }
      }

      override def next() = {
        val v = reservoir(pos)
        pos += 1
        v
      }
    }
  }

  def reset(prevReservoirSize: Int, newReservoirSize: Int,
            fullReset: Boolean): Unit = {
    if (newReservoirSize > 0) {
      // reset transient data

      // check for the end of current time-slot; if it has ended, then
      // also reset the "shortFall" and other such history
      if (fullReset) {
        totalSamples = 0
        weightage = 0.0
        prevShortFall = 0
      } else {
        // first update the shortfall for next round
        // if it has not seen much data for sometime and has fallen behind
        // a lot, then it is likely gone and there is no point in increasing
        // shortfall indefinitely (else it will over sample if seen in future)
        if (newReservoirSize <= reservoirSize ||
          prevShortFall <= (prevReservoirSize + reservoirSize)) {
          prevShortFall += (prevReservoirSize - reservoirSize)
        }
      }
      // shrink reservoir back to strataReservoirSize if required to avoid
      // growing possibly without bound (in case some strata consistently
      //   gets small number of total rows less than sample size)
      if (reservoir.length == newReservoirSize) {
        StratifiedSampler.fillArray(reservoir, EmptyRow, 0, reservoirSize)
      } else {
        reservoir = new Array[Row](newReservoirSize)
      }
      reservoirSize = 0
      batchTotalSize = 0
    }
  }
}

final class StratifiedSamplerCached(override val qcs: Array[Int],
                                    override val name: String,
                                    override val schema: StructType,
                                    override val cached: Boolean,
                                    private val cacheSize: AtomicInteger,
                                    val fraction: Double,
                                    val timeSeriesColumn: Int,
                                    val timeInterval: Int)
  extends StratifiedSampler(qcs, name, schema, cached) {

  val nbatchSamples, slotSize = new AtomicInteger
  // initialize timeSlotStart to MAX so that it will always be set first
  // time around for the slot (since every valid time will be less)
  val timeSlotStart = new AtomicLong(Long.MaxValue)
  val timeSlotEnd = new AtomicLong

  private def tsColumnTime(row: Row): Long = {
    if (timeSeriesColumn >= 0) {
      val ts = row.get(timeSeriesColumn)
      ts match {
        case tl: Long => tl
        case ti: Int => ti.toLong
        case td: java.util.Date => td.getTime
        case _ => throw new AnalysisException(
          s"""StratifiedSampler: Cannot parse 'timeSeriesColumn'=$ts""")
      }
    } else {
      System.currentTimeMillis
    }
  }

  private def setTimeSlot(row: Row) = {
    val timeSlot = tsColumnTime(row)

    var done = false
    do {
      val tsStart = timeSlotStart.get
      if (timeSlot < tsStart) {
        done = timeSlotStart.compareAndSet(tsStart, timeSlot)
      } else {
        done = true
      }
    } while (!done)

    done = false
    do {
      val tsEnd = timeSlotEnd.get
      if (timeSlot > tsEnd) {
        done = timeSlotEnd.compareAndSet(tsEnd, timeSlot)
      } else {
        done = true
      }
    } while (!done)
  }

  override def defaultValue(row: Row) = {
    val cacheSize = this.cacheSize.get
    val reservoir = new Array[Row](cacheSize)
    reservoir(0) = row.copy()
    StratifiedSampler.fillArray(reservoir, EmptyRow, 1, cacheSize)
    val sr = new StrataReservoir(1, 1, 0.0, reservoir,
      1, math.max(0, nmaxSamples.get - cacheSize))
    nmaxSamples.compareAndSet(0, 1)
    // update timeSlot start and end
    if (timeInterval > 0) {
      setTimeSlot(row)
    }
    nbatchSamples.incrementAndGet()
    slotSize.incrementAndGet()
    sr
  }

  override def mergeValue(row: Row, sr: StrataReservoir): StrataReservoir = {
    // else update meta information in current strata
    sr.batchTotalSize += 1
    val reservoirCapacity = cacheSize.get + sr.prevShortFall
    if (sr.reservoirSize >= reservoirCapacity) {
      val rnd = rng.nextInt(sr.batchTotalSize)
      // pick up this row with probability of reservoirCapacity/totalSize
      if (rnd < reservoirCapacity) {
        // replace a random row in reservoir
        sr.reservoir(rng.nextInt(reservoirCapacity)) = row.copy()
        // update timeSlot start and end
        if (timeInterval > 0) {
          setTimeSlot(row)
        }
      }
    } else {
      // if reservoir has empty slots then fill them up first
      val reservoirLen = sr.reservoir.length
      if (reservoirLen <= sr.reservoirSize) {
        // new size of reservoir will be > reservoirSize given that it
        // increases only in steps of 1 and the expression
        // reservoirLen + (reservoirLen >>> 1) + 1 will certainly be
        // greater than reservoirLen
        val newReservoir = new Array[Row](math.min(reservoirCapacity,
          reservoirLen + (reservoirLen >>> 1) + 1))
        StratifiedSampler.fillArray(newReservoir, EmptyRow,
          reservoirLen, newReservoir.length)
        System.arraycopy(sr.reservoir, 0, newReservoir,
          0, reservoirLen)
        sr.reservoir = newReservoir
      }
      sr.reservoir(sr.reservoirSize) = row.copy()
      sr.reservoirSize += 1
      sr.totalSamples += 1

      // update timeSlot start and end
      if (timeInterval > 0) {
        setTimeSlot(row)
      }

      var done = false
      val ssamples = sr.totalSamples
      do {
        val maxSamples = nmaxSamples.get
        if (ssamples > maxSamples) {
          done = nmaxSamples.compareAndSet(maxSamples, ssamples)
        } else {
          done = true
        }
      } while (!done)
      nbatchSamples.incrementAndGet()
    }
    slotSize.incrementAndGet()
    sr
  }

  override def segmentEnd[U](segment: SegmentMap[Row, StrataReservoir], init: U,
                             process: (U, Row) => U, endBatch: U => U): U = {
    // now check if we need to end the slot
    if (nbatchSamples.get >= (fraction * slotSize.get)) {
      init
    } else stratas.synchronized {
      // top-level synchronized above to avoid possible deadlocks with
      // segment locks if two threads are trying to drain cache concurrently

      // reset batch counters
      val nsamples = nbatchSamples.get
      if (nsamples > 0 && nsamples < (fraction * slotSize.get)) {
        flushCache(init, process, endBatch)
      } else {
        init
      }
    }
  }

  private def flushCache[U](init: U, process: (U, Row) => U,
                            endBatch: U => U): U = {
    // first acquire all the segment write locks so no concurrent processors
    // are in progress
    stratas.writeLock { segs =>
      nbatchSamples.set(0)
      slotSize.set(0)
      // in case the current timeSlot is over, reset nmaxSamples
      // (thus causing shortFall to clear up)
      val tsEnd = timeSlotEnd.get
      val fullReset = (tsEnd != 0) && ((tsEnd - timeSlotStart.get) >=
        (timeInterval.toLong * 1000L))
      if (fullReset) {
        nmaxSamples.set(0)
        // reset timeSlot start and end
        timeSlotStart.set(Long.MaxValue)
        timeSlotEnd.set(0)
      }
      val processSegment = foldSegment(cacheSize.get, fullReset, process) _
      endBatch(segs.foldLeft(init)(processSegment))
    }
  }

  override protected def strataReservoirSize: Int = cacheSize.get

  override def append[U](rows: Iterator[Row], init: U, process: (U, Row) => U,
                         endBatch: U => U): U = {
    if (rows.hasNext) {
      val processedResult = new ProcessRows(process, endBatch, init)
      stratas.bulkChangeValues(rows, processedResult)
      processedResult.aggregate
    } else init
  }

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    // break up input into batches of "batchSize" and bulk sample each batch
    new Iterator[Row] {
      val rows = items
      var hasItems = rows.hasNext
      var mapIterator: Iterator[Row] = Iterator.empty

      val batchSize = 1000
      val buffer = new mutable.ArrayBuffer[Row](batchSize)
      var bufferLen = 0
      val sbufSize = math.min(batchSize, (batchSize * fraction * 10).toInt)
      val sampleBuffer = new mutable.ArrayBuffer[Row](sbufSize)
      var finished = false

      private def buildSampleIterator(): Iterator[Row] = {
        val sampleBuffer = this.sampleBuffer
        if (sampleBuffer.nonEmpty) sampleBuffer.clear()
        // bulk sample the buffer
        append[Unit](buffer.iterator, (), {
          (u, sampledRow) => sampleBuffer += sampledRow; u
        }, identity)
        sampleBuffer.iterator
      }

      override final def hasNext: Boolean = {
        if (mapIterator.hasNext) {
          return true
        }
        val rows = this.rows
        val buffer = this.buffer
        while (hasItems) {
          val row = rows.next()
          hasItems = rows.hasNext
          // now check if we need to end the slot and return current samples
          if (bufferLen < batchSize) {
            buffer += row
            bufferLen += 1
          }
          else {
            mapIterator = buildSampleIterator()
            buffer.clear()
            bufferLen = 0
            if (mapIterator.hasNext) return true
          }
        }
        if (bufferLen > 0) {
          mapIterator = buildSampleIterator()
          buffer.clear()
          bufferLen = 0
        }
        if (mapIterator.hasNext) {
          return true
        }
        if (!finished) {
          finished = true
          // force flush the cache
          stratas.synchronized {
            val sampleBuffer = this.sampleBuffer
            if (sampleBuffer.nonEmpty) sampleBuffer.clear()
            flushCache[Unit]((), {
              (u, sampledRow) => sampleBuffer += sampledRow; u
            }, identity)
            mapIterator = sampleBuffer.iterator
          }
          mapIterator.hasNext
        }
        else false
      }

      override final def next(): Row = {
        mapIterator.next()
      }
    }
  }

  override def clone: StratifiedSamplerCached =
    new StratifiedSamplerCached(qcs, name, schema, false, cacheSize,
      fraction, timeSeriesColumn, timeInterval)
}

final class StratifiedSamplerReservoir(override val qcs: Array[Int],
                                       override val name: String,
                                       override val schema: StructType,
                                       override val cached: Boolean,
                                       private val reservoirSize: Int)
  extends StratifiedSampler(qcs, name, schema, cached) {

  override def defaultValue(row: Row) = {
    val strataSize = this.reservoirSize
    // create new strata if required
    val reservoir = new Array[Row](strataSize)
    reservoir(0) = row.copy()
    StratifiedSampler.fillArray(reservoir, EmptyRow, 1, strataSize)
    new StrataReservoir(1, 1, 0.0, reservoir, 1, 0)
  }

  override def mergeValue(row: Row, sr: StrataReservoir): StrataReservoir = {
    val strataSize = this.reservoirSize
    // else update meta information in current strata
    sr.batchTotalSize += 1
    if (sr.reservoirSize >= strataSize) {
      // copy into the reservoir as per probability (strataSize/totalSize)
      val rnd = rng.nextInt(sr.batchTotalSize)
      if (rnd < strataSize) {
        // pick up this row and replace a random one from reservoir
        sr.reservoir(rng.nextInt(strataSize)) = row.copy()
      }
    } else {
      // always copy into the reservoir for this case
      sr.reservoir(sr.reservoirSize) = row.copy()
      sr.reservoirSize += 1
    }
    sr
  }

  override protected def strataReservoirSize: Int = reservoirSize

  override def append[U](rows: Iterator[Row], init: U, process: (U, Row) => U,
                         endBatch: U => U): U = {
    if (rows.hasNext) {
      val processedResult = new ProcessRows(process, endBatch, init)
      stratas.bulkChangeValues(rows, processedResult)
      processedResult.aggregate
    } else init
  }

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    // break up into batches of some size
    val batchSize = 1000
    val buffer = new mutable.ArrayBuffer[Row](batchSize)
    items.foreach { row =>
      if (buffer.length < batchSize) {
        buffer += row
      } else {
        // bulk append to sampler
        append[Unit](buffer.iterator, (), null, null)
        buffer.clear()
      }
    }
    // append any remaining in buffer
    if (buffer.nonEmpty) {
      append[Unit](buffer.iterator, (), null, null)
    }

    // finally iterate over all the strata reservoirs
    stratas.toValues.flatMap(_.iterator(reservoirSize,
      reservoirSize, fullReset = false)).toIterator
  }

  override def clone: StratifiedSamplerReservoir =
    new StratifiedSamplerReservoir(qcs, name, schema, false, reservoirSize)
}
