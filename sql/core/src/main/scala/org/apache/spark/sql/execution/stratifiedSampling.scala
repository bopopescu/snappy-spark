package org.apache.spark.sql.execution

import org.apache.spark.Logging
import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.expressions.{Attribute, EmptyRow}
import org.apache.spark.sql.collection.MultiColumnOpenHashMap
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.random.{RandomSampler, XORShiftRandom}

import scala.collection.Iterator
import scala.collection.mutable.OpenHashMap
import scala.language.reflectiveCalls
import scala.util.Sorting

/**
 * Perform stratified sampling given a Query-Column-Set (QCS). This variant
 * can also use a fixed fraction to be sampled instead of fixed number of
 * total samples since it is eventually designed to be used with streaming data.
 */
case class StratifiedSample(options: Map[String, Any], tableSchema: StructType,
                            child: SparkPlan)
    extends UnaryNode {

  self =>

  override def output: Seq[Attribute] = child.output

  protected override def doExecute(): RDD[Row] = {
    new PartitionwiseSampledRDD[Row, Row](child.execute(),
      StratifiedSampler(options, tableSchema), true)
  }
}

object StratifiedSampler {

  private val globalMap = new OpenHashMap[String, StratifiedSampler]

  implicit class StringExtensions(val s: String) extends AnyVal {
    def ci = new {
      def unapply(other: String) = s.equalsIgnoreCase(other)
    }
  }

  def fillArray[T](a: Array[T], v: T, start: Int, endP1: Int) = {
    start until endP1 foreach { a(_) = v }
  }

  def columnIndex(col: String, cols: Array[String]) = {
    val colT = col.trim
    val colIndex = (0 until cols.length).foldLeft(-1) {
      case (idx, i) => {
        if (colT.equalsIgnoreCase(cols(i))) i else idx
      }
    }
    if (colIndex >= 0) colIndex
    else throw new AnalysisException(
      s"""StratifiedSampler: Cannot resolve column name "$col" among
            (${cols.mkString(", ")})""")
  }

  def qcsOf(qa: Array[String], cols: Array[String]) = {
    val colIndexes = qa.map { columnIndex(_, cols) }
    Sorting.quickSort(colIndexes)
    colIndexes
  }

  def qcsOf(qa: Array[String], schema: StructType): Array[Int] =
    qcsOf(qa, schema.fieldNames)

  def apply(options: Map[String, Any], schema: StructType): StratifiedSampler = {
    val nameTest = "name".ci
    val qcsTest = "qcs".ci
    val fracTest = "fraction".ci
    val csizeTest = "cacheSize".ci
    val rsizeTest = "reservoirTest".ci
    val timeSeriesColumnTest = "timeSeriesColumn".ci
    val timeIntervalTest = "timeInterval".ci
    val timeIntervalSpec = "([0-9]+)(s|m|h)".r
    val cols = schema.fieldNames

    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (qcs, fraction, cacheSize) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (nm, qcs, fraction, cacheSize, tsCol, timeInterval) = options.foldLeft(
      "", Array.emptyIntArray, 0.0, 0, -1, 0) {
        case ((n, qs, fr, sz, ts, ti), (opt, optV)) => {
          opt match {
            case qcsTest() => optV match {
              case qi: Array[Int] => (n, qi, fr, sz, ts, ti)
              case q: String      => (n, qcsOf(q.split(","), cols), fr, sz, ts, ti)
              case _ => throw new AnalysisException(
                s"""StratifiedSampler: Cannot parse 'qcs'="$optV" among
                  (${cols.mkString(", ")})""")
            }
            case fracTest() => optV match {
              case fd: Double => (n, qs, fd, sz, ts, ti)
              case fs: String => (n, qs, fs.toDouble, sz, ts, ti)
              case ff: Float  => (n, qs, ff.toDouble, sz, ts, ti)
              case fi: Int    => (n, qs, fi.toDouble, sz, ts, ti)
              case fl: Long   => (n, qs, fl.toDouble, sz, ts, ti)
              case _ => throw new AnalysisException(
                s"""StratifiedSampler: Cannot parse double 'fraction'=$optV""")
            }
            case csizeTest() | rsizeTest() => optV match {
              case si: Int    => (n, qs, fr, si, ts, ti)
              case ss: String => (n, qs, fr, ss.toInt, ts, ti)
              case sl: Long   => (n, qs, fr, sl.toInt, ts, ti)
              case _ => throw new AnalysisException(
                s"""StratifiedSampler: Cannot parse int 'cacheSize'=$optV""")
            }
            case timeSeriesColumnTest() => optV match {
              case tss: String => (n, qs, fr, sz, columnIndex(tss, cols), ti)
              case tsi: Int    => (n, qs, fr, sz, tsi, ti)
              case _ => throw new AnalysisException(
                s"""StratifiedSampler: Cannot parse 'timeSeriesColumn'=$optV""")
            }
            case timeIntervalTest() => optV match {
              case tii: Int  => (n, qs, fr, sz, ts, tii)
              case til: Long => (n, qs, fr, sz, ts, til.toInt)
              case tis: String => tis match {
                case timeIntervalSpec(interval, unit) =>
                  unit match {
                    case "s" => (n, qs, fr, sz, ts, interval.toInt)
                    case "m" => (n, qs, fr, sz, ts, interval.toInt * 60)
                    case "h" => (n, qs, fr, sz, ts, interval.toInt * 3600)
                    case _ => throw new AssertionError(
                      s"""unexpected regex match 'unit'=$unit""")
                  }
                case _ => throw new AnalysisException(
                  s"""StratifiedSampler: Cannot parse 'timeInterval'=$tis""")
              }
              case _ => throw new AnalysisException(
                s"""StratifiedSampler: Cannot parse 'timeInterval'=$optV""")
            }
            case nameTest() => (optV.toString, qs, fr, sz, ts, ti)
            case _ => throw new AnalysisException(
              s"""StratifiedSampler: Unknown option "$opt"""")
          }
        }
      }

    if (nm.length > 0) {
      globalMap.getOrElse(nm, {
        val sampler = newSampler(qcs, fraction, cacheSize, tsCol,
          timeInterval, schema)
        // insert into global map
        globalMap(nm) = sampler
        sampler
      })
    } else {
      newSampler(qcs, fraction, cacheSize, tsCol, timeInterval, schema)
    }
  }

  def removeSampler(name: String): Option[StratifiedSampler] = {
    globalMap.remove(name)
  }

  private def newSampler(qcs: Array[Int], fraction: Double, cacheSize: Int,
      tsCol: Int, timeInterval: Int, schema: StructType): StratifiedSampler = {
    if (qcs.length == 0)
      throw new AnalysisException("StratifiedSampler: QCS is empty")
    else if (fraction > 0.0)
      new StratifiedSamplerCached(qcs, math.max(cacheSize, 1), schema,
        fraction, tsCol, timeInterval)
    else if (cacheSize > 0)
      new StratifiedSamplerFullReservoir(qcs, cacheSize, schema)
    else throw new AnalysisException(
      s"""StratifiedSampler: 'fraction'=$fraction 'cacheSize'=$cacheSize""")
  }
}

abstract class StratifiedSampler(val qcs: Array[Int], val cacheSize: Int,
                                 val schema: StructType)
  extends RandomSampler[Row, Row] with Logging {

  /**
   * Map of each strata key (i.e. a unique combination of values of columns
   * in qcs) to related metadata and reservoir
   */
  protected final val stratas = {
    val types: Array[DataType] = new Array[DataType](qcs.length)
    for (i <- 0 until qcs.length) {
      types(i) = schema(qcs(i)).dataType
    }
    new MultiColumnOpenHashMap[StrataReservoir](qcs, types)
  }

  /**
   * Random number generator for sampling.
   */
  protected final val rng = new XORShiftRandom

  /** Keeps track of the maximum number of samples in a strata seen so far */
  protected var nmaxSamples = 0

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  def append(row: Row): Boolean

  /*
  private val rddToCounter = new OpenHashMap[Int, Int]

  def roundCounter(rddId: Int) : Int = {
    val v = rddToCounter.get(rddId)
    if (v.isDefined)
      rddToCounter.put(rddId, v.get + 1)
  }
  */

  def iterator: Iterator[Row] = iterator(nmaxSamples)

  protected final def iterator(maxSamples: Int) = {
    stratas.valuesIterator.flatMap(sr => new Iterator[Row] {

      val reservoir = sr.reservoir
      val nsamples = sr.reservoirSize
      var pos = 0

      override def hasNext: Boolean = {
        if (pos < nsamples) {
          return true
        } else if (cacheSize > 0) {
          // reset transient data

          // check for the end of current time-slot indicated by maxSamples=0;
          // if it has ended, then reset "prevShortFall" and other such history
          if (maxSamples == 0) {
            sr.nsamples = 0
            sr.ntotalSize = 0
            sr.weightage = 0.0
            sr.prevShortFall = 0
          } else {
            // first update the shortfall for next round
            sr.prevShortFall = math.max(0, maxSamples - sr.nsamples)
          }
          // shrink reservoir back to cacheSize if required to avoid
          // growing possibly without bound (in case some strata consistently
          //   gets small number of total rows less than sample size)
          if (reservoir.length == cacheSize) {
            StratifiedSampler.fillArray(reservoir, EmptyRow, 0, nsamples)
          } else {
            sr.reservoir = new Array[Row](cacheSize)
          }
          sr.reservoirSize = 0
          sr.batchTotalSize = 0
          false
        } else {
          false
        }
      }

      override def next() = {
        val v = reservoir(pos)
        pos += 1
        v
      }
    })
  }
}

// TODO: optimize by having metadata as multiple columns like key;
// add a good sparse array implementation

/**
 * For each strata (i.e. a unique set of values for QCS), keep a set of
 * meta-data including number of samples collected, total number of rows
 * in the strata seen so far, the QCS key, reservoir of samples etc.
 */
final class StrataReservoir(var nsamples: Int, var ntotalSize: Int,
                            var weightage: Double, var reservoir: Array[Row],
                            var reservoirSize: Int, var batchTotalSize: Int,
                            var prevShortFall: Int) {
}

final class StratifiedSamplerCached(override val qcs: Array[Int],
                                    override val cacheSize: Int,
                                    override val schema: StructType,
                                    val fraction: Double,
                                    val timeSeriesColumn: Int,
                                    val timeInterval: Int)
    extends StratifiedSampler(qcs, cacheSize, schema) {

  var nbatchSamples, slotSize = 0
  var timeSlotStart: Long = 0

  private def tsColumnTime(row: Row): Long = {
    if (timeSeriesColumn >= 0) {
      val ts = row.get(timeSeriesColumn)
      ts match {
        case tl: Long          => tl
        case ti: Int           => ti.toLong
        case td: java.sql.Date => td.getTime
        case _ => throw new AnalysisException(
          s"""StratifiedSampler: Cannot parse 'timeSeriesColumn'=$ts""")
      }
    } else {
      System.currentTimeMillis
    }
  }

  override def append(row: Row): Boolean = {
    stratas.changeKeyValue(row,
      () => {
        // create new strata
        val newRow = row.copy
        val reservoir = new Array[Row](cacheSize)
        reservoir(0) = newRow
        StratifiedSampler.fillArray(reservoir, EmptyRow, 1, cacheSize)
        val sr = new StrataReservoir(1, 1, 0.0, reservoir,
          1, 1, math.max(0, nmaxSamples - cacheSize))
        if (nmaxSamples == 0) {
          nmaxSamples = 1
        }
        nbatchSamples += 1
        (newRow, sr)
      },
      sr => {
        // else update meta information in current strata
        sr.ntotalSize += 1
        sr.batchTotalSize += 1
        val reservoirCapacity = cacheSize + sr.prevShortFall
        if (sr.reservoirSize >= reservoirCapacity) {
          val rnd = rng.nextInt(sr.batchTotalSize)
          // pick up this row with probability of reservoirSize/capacity
          if (rnd < reservoirCapacity) {
            // replace a random row in reservoir
            sr.reservoir(rng.nextInt(reservoirCapacity)) = row.copy
          }
        } else {
          // if reservoir has empty slots then fill them up first
          val reservoirLen = sr.reservoir.length
          if (reservoirLen <= sr.reservoirSize) {
            val newReservoir = new Array[Row](math.min(reservoirCapacity,
              reservoirLen + (reservoirLen >>> 1) + 1))
            StratifiedSampler.fillArray(newReservoir, EmptyRow,
                reservoirLen, newReservoir.length)
            System.arraycopy(sr.reservoir, 0, newReservoir,
              0, reservoirLen)
            sr.reservoir = newReservoir
          }
          sr.reservoir(sr.reservoirSize) = row.copy
          sr.reservoirSize += 1
          sr.nsamples += 1
          if (sr.nsamples > nmaxSamples) {
            nmaxSamples = sr.nsamples
          }
          nbatchSamples += 1
        }
      })

    // initialize the start of timeSlot if configured
    if (slotSize == 0 && timeSlotStart == 0 && timeInterval > 0) {
      timeSlotStart = tsColumnTime(row)
    }
    slotSize += 1
    // now check if we need to end the slot
    if (nbatchSamples > (fraction * slotSize)) {
      false
    } else {
      // reset batch counters
      nbatchSamples = 0
      slotSize = 0
      // in case the current timeSlot is over, reset nmaxSamples
      // (thus causing shortFall to clear up)
      if (timeSlotStart > 0) {
        val timeSlot = tsColumnTime(row)
        if ((timeSlot - timeSlotStart) >= (timeInterval.toLong * 1000L)) {
          nmaxSamples = 0
        }
      }
      true
    }
  }

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    // "purely non-functional" code below but more efficient
    /*
    new Iterator[Row] {
      var hasItems = items.hasNext
      var mapIterator: Iterator[Row] = Iterator.empty

      override def hasNext: Boolean = {
        if (mapIterator.hasNext) {
          return true
        }
        while (hasItems) {
          val row = items.next
          hasItems = items.hasNext
          // now check if we need to end the slot and return current samples
          if (!append(row)) {
            // if we reached end of source, then initialize the iterator
            // for any remaining cached samples
            if (!hasItems) {
              mapIterator = iterator
            }
          } else {
            // return current samples and clear for reuse
            mapIterator = iterator
            if (mapIterator.hasNext) return true
          }
        }
        return mapIterator.hasNext
      }

      override def next() = {
        mapIterator.next
      }
    }
    */

    items.flatMap(row => {
      if (append(row)) {
        Iterator.empty
      } else {
        // return current samples and clear for reuse
        iterator
      }
    }) ++ iterator(0)
  }

  override def clone: StratifiedSamplerCached = new StratifiedSamplerCached(
    qcs, cacheSize, schema, fraction, timeSeriesColumn, timeInterval)
}

final class StratifiedSamplerFullReservoir(override val qcs: Array[Int],
                                           override val cacheSize: Int,
                                           override val schema: StructType)
    extends StratifiedSampler(qcs, cacheSize, schema) {

  override def append(row: Row): Boolean = {
    val strataSize = this.cacheSize
    stratas.changeKeyValue(row,
      () => {
        // create new strata if required
        val newRow = row.copy
        val reservoir = new Array[Row](strataSize)
        reservoir(0) = newRow
        StratifiedSampler.fillArray(reservoir, EmptyRow, 1, strataSize)
        val sr = new StrataReservoir(1, 1, 0.0, reservoir, 1, 1, 0)
        (newRow, sr)
      },
      sr => {
        // else update meta information in current strata
        sr.ntotalSize += 1
        if (sr.reservoirSize >= strataSize) {
          // copy into the reservoir as per probability (strataSize/totalSize)
          val rnd = rng.nextInt(sr.ntotalSize)
          if (rnd < strataSize) {
            // pick up this row and replace a random one from reservoir
            sr.reservoir(rng.nextInt(strataSize)) = row.copy
          }
        } else {
          // always copy into the reservoir for this case
          sr.reservoir(sr.reservoirSize) = row.copy
          sr.reservoirSize += 1
        }
      })
    false
  }

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    items.foreach(append(_))
    // finally iterate over all the strata reservoirs
    iterator(0)
  }

  override def clone: StratifiedSamplerFullReservoir =
    new StratifiedSamplerFullReservoir(qcs, cacheSize, schema)
}
