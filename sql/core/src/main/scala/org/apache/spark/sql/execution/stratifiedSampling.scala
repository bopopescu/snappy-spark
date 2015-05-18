package org.apache.spark.sql.execution

import java.util.ArrayDeque
import scala.collection.Iterator
import scala.collection.mutable.PriorityQueue
import scala.util.hashing.MurmurHash3
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PartitionwiseSampledRDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.EmptyRow
import org.apache.spark.sql.collection.MultiColumnOpenHashMap
import org.apache.spark.util.random.RandomSampler
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType

/**
 * Perform stratified sampling given a Query-Column-Set (QCS). This variant
 * uses a fixed fraction to be sampled instead of fixed number of total samples
 * since it is eventually designed to be used with streaming data.
 */
case class StratifiedSample(qcs: Array[Int], fraction: Double,
                            tableSchema: StructType, child: SparkPlan)
    extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override def execute(): RDD[Row] = {
    new PartitionwiseSampledRDD[Row, Row](child.execute(),
      new StratifiedSampler(qcs, fraction, tableSchema), true, 1)
  }
}

/**
 * Creates a reusable iterator which produces a single element. Provides a
 * `+=` method to fill in a new element and reuse the iterator afresh.
 *
 * @param elem the element
 *
 * @return An iterator which has only a single item `elem`
 */
final class SingleReusableIterator[A](var elem: A) extends Iterator[A] {

  private var hasnext = true

  override def hasNext: Boolean = this.hasnext

  override def next(): A =
    if (this.hasnext) {
      this.hasnext = false;
      this.elem
    } else {
      Iterator.empty.next()
    }

  def +=(e: A): SingleReusableIterator[A] = {
    this.elem = e
    this.hasnext = true
    this
  }
}

// TODO: optimize by having metadata as multiple columns like key;
// add a good sparse array implementation
/**
 * For each strata (i.e. a unique set of values for QCS), keep a set of
 * meta-data including number of samples collected, total number of rows
 * in the strata seen so far, the QCS key, a cached recent sample to use
 * in case no new sample is seen for a while etc.
 */
private final class StrataMetadata(var nSamples: Int, var nTotalSize: Int,
                                   var weightage: Double, var pendingRow: Row,
                                   var copyAfter: Int, var batch: Int,
                                   var refreshPending: Int) {
}

private object StrataMetadata {
  val empty: StrataMetadata = new StrataMetadata(0, 0, 0.0,
    EmptyRow, 0, 0, 0)
}

final class StratifiedSampler(val qcs: Array[Int], val fraction: Double,
                              val schema: StructType)
    extends RandomSampler[Row, Row] {

  /**
   * This is the size of single batch out of which a single sample for
   * "most wanted" strata will be picked.
   */
  private val batchSize = (1.0 / this.fraction).toInt

  /**
   * Map of each strata key (i.e. a unique combination of values of columns
   * in qcs) to related metadata
   */
  private val stratas = {
    val types: Array[DataType] = new Array[DataType](qcs.length)
    for (i <- 0 until qcs.length) {
      types(i) = schema(qcs(i)).dataType
    }
    new MultiColumnOpenHashMap[StrataMetadata](qcs, types)
  }

  /**
   * A copy of row is made into `StrataMetadata.pendingRow` once every these
   * many times for use later in case no row for the current strata is found
   * in current batch.
   */
  private val copyFrequency = 50

  /**
   * A copy of row is forced after having traversed these many batches
   * to not hold on to very old rows in `StrataMetadata.pendingRow` for
   * strata that have very low frequency and `copyFrequency` may be too
   * large and thus inappropriate.
   */
  private val pendingRefreshBatch = 10

  /**
   * A queue of stratas ordered by number of samples seen so far which
   * helps determine the next strata for which a sample needs to be returned.
   */
  private val strataPriority = new ArrayDeque[StrataMetadata]
  /*
  private val strataPriority = new PriorityQueue[StrataMetadata]()(
    new Ordering[StrataMetadata] {
      override def compare(a: StrataMetadata, b: StrataMetadata) =
        Integer.compare(a.nSamples, b.nSamples)
    })
  */

  override def setSeed(seed: Long) {
    // nothing to be done for seed
  }

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    val qcst = this.qcs
    val nQCS = this.qcs.length
    var currentStrata = StrataMetadata.empty
    var nCurrentBatch = this.batchSize
    val singleIter = new SingleReusableIterator[Row](EmptyRow)
    var forceRefreshCount, batchCount = 0
    var currentMaxSampleCount_1 = 0

    items.flatMap(row => {
      var newMd: StrataMetadata = StrataMetadata.empty
      val currentMd = this.stratas.changeKeyValue(row,
        () => {
          // create new strata if required
          val newRow = row.copy
          newMd = new StrataMetadata(1, 1, 0.0, newRow,
            copyFrequency, batchCount, forceRefreshCount)
          // push the new strata at the end into the priority list
          this.strataPriority addLast newMd
          (newRow, newMd)
        },
        md => {
          // else update meta information in current strata; make a new
          // copy of pendingRow to use in case we do not find value of
          // this strata later when required
          md.nTotalSize += 1
          // avoid making a copy of Row everytime to displace older one
          // and instead do it only once every 'copyFrequency' times
          if (md.copyAfter != 0 && md.refreshPending == forceRefreshCount) {
            md.copyAfter -= 1
          } else {
            md.pendingRow = row.copy
            md.copyAfter = copyFrequency
            md.refreshPending = forceRefreshCount
          }
        })

      if (newMd ne StrataMetadata.empty) {
        singleIter += row
      } // now get the current strata being searched for from the priority queue
      // -ve value in nCurrentBatch indicates that current strata value has
      // already been received and does not need to be done for this batch
      else if (nCurrentBatch < 0) {
        nCurrentBatch += 1
        if (nCurrentBatch == 0) {
          currentStrata = StrataMetadata.empty
          nCurrentBatch = this.batchSize
          batchCount += 1
          if ((batchCount % this.pendingRefreshBatch) == 0) {
            forceRefreshCount += 1
          }
        }
        if (currentMd.nSamples < currentMaxSampleCount_1
          && currentMd.batch != batchCount) {
          currentMd.nSamples += 1
          currentMd.batch = batchCount
          singleIter += row
        } else {
          Iterator.empty
        }
      } else {
        if (currentStrata eq StrataMetadata.empty) {
          currentStrata = this.strataPriority.removeLast
        }
        // return the value if it matches current strata
        // note that we can do reference comparison of the StrataMetadata
        // object instead of equals on the key since the same object
        // is both within `stratas` and `strataPriority`
        if (currentMd eq currentStrata) {
          if (nCurrentBatch <= 1) {
            nCurrentBatch = this.batchSize
          } else {
            nCurrentBatch = -nCurrentBatch + 1
          }
          if (currentMaxSampleCount_1 < currentStrata.nSamples) {
            currentMaxSampleCount_1 = currentStrata.nSamples
          }
          currentStrata.nSamples += 1
          this.strataPriority addLast currentStrata
          singleIter += row
        } else {
          nCurrentBatch -= 1
          val skipCurrentRow =
            if (currentMd.nSamples < currentMaxSampleCount_1
              && currentMd.batch != batchCount) {
              currentMd.nSamples += 1
              currentMd.batch = batchCount
              false
            } else {
              true
            }
          if (nCurrentBatch == 0) {
            // did not find any sample in current strata so use pending value
            val md = currentStrata
            val pendingRow: Row = md.pendingRow
            val itr: Iterator[Row] =
              if (pendingRow ne EmptyRow) {
                // reset stored row and copy frequency so next row in the
                // strata gets copied
                md.pendingRow = EmptyRow
                md.copyAfter = 0
                if (currentMaxSampleCount_1 < md.nSamples) {
                  currentMaxSampleCount_1 = md.nSamples
                }
                md.nSamples += 1
                if (skipCurrentRow) {
                  singleIter += pendingRow
                } else {
                  Iterator(row, pendingRow)
                }
              } else if (skipCurrentRow) {
                Iterator.empty
              } else {
                singleIter += row
              }
            this.strataPriority addLast md
            currentStrata = StrataMetadata.empty
            nCurrentBatch = this.batchSize
            batchCount += 1
            if ((batchCount % this.pendingRefreshBatch) == 0) {
              forceRefreshCount += 1
            }
            itr
          } else if (skipCurrentRow) {
            Iterator.empty
          } else {
            singleIter += row
          }
        }
      }
    })
  }

  override def clone: StratifiedSampler =
    new StratifiedSampler(qcs, fraction, schema)
}
