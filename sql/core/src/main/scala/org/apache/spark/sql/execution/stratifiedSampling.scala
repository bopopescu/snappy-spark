package org.apache.spark.sql.execution

import scala.collection.Iterator
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PartitionwiseSampledRDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.EmptyRow
import org.apache.spark.sql.collection.MultiColumnOpenHashMap
import org.apache.spark.util.random.RandomSampler
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.sql.AnalysisException
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

  override def execute(): RDD[Row] = {
    new PartitionwiseSampledRDD[Row, Row](child.execute(),
      StratifiedSampler.newSampler(options, tableSchema), true)
  }
}

abstract class StratifiedSampler(val qcs: Array[Int], val schema: StructType)
    extends RandomSampler[Row, Row] {

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

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  def iterator = StrataReservoir.mapIterator(stratas, 0, 0)
}

object StratifiedSampler {

  def qcsOf(qa: Array[String], schema: StructType) = {
    val cols = schema.fieldNames
    val colIndexes = qa.map { col: String =>
      val colIndex = cols.indexOf(col.trim)
      if (colIndex >= 0) colIndex
      else throw new AnalysisException(
        s"""StratifiedSampler: Cannot resolve column name "$col" among
            (${schema.fieldNames.mkString(", ")})""")
    }
    Sorting.quickSort(colIndexes)
    colIndexes
  }

  def newSampler(options: Map[String, Any], schema: StructType) = {
    val defOpts = (null.asInstanceOf[Array[Int]], 0.0, 0)
    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (qcs, fraction, cacheSize) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (qcs, fraction, cacheSize) = options.foldLeft(defOpts) {
      case ((cols, frac, sz), (opt, optV)) => {
        opt match {
          case "qcs" => optV match {
            case qi: Array[Int]    => (qi, frac, sz)
            case qa: Array[String] => (qcsOf(qa, schema), frac, sz)
            case qs: String        => (qcsOf(qs.split(","), schema), frac, sz)
            case _ => throw new AnalysisException(
              s"""StratifiedSampler: Cannot parse 'qcs'="$optV" among
                  (${schema.fieldNames.mkString(", ")})""")
          }
          case "fraction" => optV match {
            case fd: Double => (cols, fd, sz)
            case ff: Float  => (cols, ff.toDouble, sz)
            case fi: Int    => (cols, fi.toDouble, sz)
            case fl: Long   => (cols, fl.toDouble, sz)
            case fs: String => (cols, fs.toDouble, sz)
            case _ => throw new AnalysisException(
              s"""StratifiedSampler: Cannot parse double 'fraction'=$optV""")
          }
          case "cacheSize" | "reservoirSize" => optV match {
            case si: Int    => (cols, frac, si)
            case sl: Long   => (cols, frac, sl.toInt)
            case ss: String => (cols, frac, ss.toInt)
            case _ => throw new AnalysisException(
              s"""StratifiedSampler: Cannot parse int 'cacheSize'=$optV""")
          }
          case _ => throw new AnalysisException(
            s"""StratifiedSampler: Unknown option "$opt"""")
        }
      }
    }

    if (qcs == null)
      throw new AnalysisException("StratifiedSampler: QCS is null")
    else if (fraction > 0.0)
      new StratifiedSamplerCached(qcs, schema, fraction, cacheSize)
    else if (cacheSize > 0)
      new StratifiedSamplerFullReservoir(qcs, schema, cacheSize)
    else throw new AnalysisException(
      s"""StratifiedSampler: 'fraction'=$fraction 'cacheSize'=$cacheSize""")
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

private object StrataReservoir {

  def mapIterator(stratas: MultiColumnOpenHashMap[StrataReservoir],
                  cacheSize: Int, maxSamples: Int) = {
    stratas.valuesIterator.flatMap(sr => new Iterator[Row] {

      val reservoir = sr.reservoir
      val nsamples = sr.reservoirSize
      var pos = 0

      override def hasNext: Boolean = {
        if (pos < nsamples) {
          return true
        } else if (cacheSize > 0) {
          // reset transient data

          // first update the shortfall for next round
          sr.prevShortFall = math.max(0, maxSamples - sr.nsamples)
          // shrink reservoir back to cacheSize if required to avoid
          // growing possibly without bound (in case some strata consistently
          //   gets small number of total rows less than sample size)
          if (reservoir.length == cacheSize) {
            0 until nsamples foreach { i => reservoir(i) = EmptyRow }
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

final class StratifiedSamplerCached(override val qcs: Array[Int],
                                    override val schema: StructType,
                                    val fraction: Double, val cacheSize: Int)
    extends StratifiedSampler(qcs, schema) {

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    // "purely non-functional" code below but more efficient
    /*
    new Iterator[Row] {
      val rng = StratifiedSampler.this.rng
      val stratas = StratifiedSampler.this.stratas
      val fraction = StratifiedSampler.this.fraction
      val cacheSize = StratifiedSampler.this.cacheSize
      var nbatchSamples, slotSize, nmaxSamples = 0

      var hasItems = items.hasNext
      var mapIterator: Iterator[Row] = Iterator.empty

      override def hasNext: Boolean = {
        if (mapIterator.hasNext) {
          return true
        }
        while (hasItems) {
          val row = items.next
          hasItems = items.hasNext
          stratas.changeKeyValue(row,
            () => {
              // create new strata
              val newRow = row.copy
              val reservoir = new Array[Row](cacheSize)
              reservoir(0) = newRow
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

          slotSize += 1
          // now check if we need to end the slot and return current samples
          if (nbatchSamples > (fraction * slotSize)) {
            // if we reached end of source, then initialize the iterator
            // for any remaining cached samples
            if (!hasItems) {
              mapIterator = StrataReservoir.mapIterator(stratas,
                cacheSize, nmaxSamples)
            }
          } else {
            // reset batch counters
            nbatchSamples = 0
            slotSize = 0
            // return current samples and clear for reuse
            mapIterator = StrataReservoir.mapIterator(stratas,
                cacheSize, nmaxSamples)
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

    val rng = this.rng
    val stratas = this.stratas
    val fraction = this.fraction
    val cacheSize = this.cacheSize
    var nbatchSamples, slotSize, nmaxSamples = 0

    items.flatMap(row => {
      stratas.changeKeyValue(row,
        () => {
          // create new strata
          val newRow = row.copy
          val reservoir = new Array[Row](cacheSize)
          reservoir(0) = newRow
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
              System.arraycopy(sr.reservoir, 0, newReservoir, 0, reservoirLen)
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

      slotSize += 1
      if (nbatchSamples > (fraction * slotSize)) {
        Iterator.empty
      } else {
        // reset batch counters
        nbatchSamples = 0
        slotSize = 0
        // return current samples and clear for reuse
        StrataReservoir.mapIterator(stratas, cacheSize, nmaxSamples)
      }
    }) ++ StrataReservoir.mapIterator(stratas, 0, 0)
  }

  override def clone: StratifiedSamplerCached =
    new StratifiedSamplerCached(qcs, schema, fraction, cacheSize)
}

final class StratifiedSamplerFullReservoir(override val qcs: Array[Int],
                                           override val schema: StructType,
                                           val strataSize: Int)
    extends StratifiedSampler(qcs, schema) {

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    val rng = this.rng
    val strataSize = this.strataSize

    items.foreach(row => {
      this.stratas.changeKeyValue(row,
        () => {
          // create new strata if required
          val newRow = row.copy
          val reservoir = new Array[Row](strataSize)
          reservoir(0) = newRow
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
    })

    // finally iterate over all the strata reservoirs
    StrataReservoir.mapIterator(this.stratas, 0, 0)
  }

  override def clone: StratifiedSamplerFullReservoir =
    new StratifiedSamplerFullReservoir(qcs, schema, strataSize)
}
