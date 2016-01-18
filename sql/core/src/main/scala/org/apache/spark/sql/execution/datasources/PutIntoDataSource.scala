package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources._

/**
 * Upserts the results of `query` in to a relation that extends [[RowPutRelation]].
 */
private[sql] case class PutIntoDataSource(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean)
    extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[RowPutRelation]
    val data = DataFrame(sqlContext, query)
    // Apply the schema of the existing table to the new data.
    val df = sqlContext.internalCreateDataFrame(data.queryExecution.toRdd, logicalRelation.schema)
    relation.insert(df, overwrite)

    // Invalidate the cache.
    sqlContext.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}