package org.apache.spark.sql

import scala.reflect.ClassTag

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.StratifiedSampler
import org.apache.spark.{Partition, TaskContext}

/**
 * Encapsulates an RDD over all the cached samples for a sampled table.
 *
 * Created by Soubhik on 5/13/15.
 */
object CachedRDD {

  def samplerCache(name: String, sqlContext: SQLContext) = {
    Utils.mapExecutors(sqlContext, () =>
      StratifiedSampler(name) match {
        case Some(ss) => ss.iterator
        case None => Iterator.empty
      })
  }
}

private[sql] final class MapPartitionsPreserveRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T], f: (TaskContext, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false)
    extends MapPartitionsRDD[U, T](prev, f, preservesPartitioning) {

  // TODO [sumedh] why doesn't the standard MapPartitionsRDD do this???
  override def getPreferredLocations(split: Partition) =
    firstParent[T].preferredLocations(split)
}

class DummyRDD(sqlContext: SQLContext)
    extends RDD[Row](sqlContext.sparkContext, Nil) {

  /**
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    Iterator.empty

  /**
   * Implemented by subclasses to return the set of partitions in this RDD.
   * This method will only be called once, so it is safe to implement
   * a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = Array.empty
}
