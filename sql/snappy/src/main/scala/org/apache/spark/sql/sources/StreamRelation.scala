package org.apache.spark.sql.sources

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExecutedCommand, RunnableCommand, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.{UnionDStream, DStream}
import org.apache.spark.streaming.Duration
import org.apache.spark.util.Utils

/**
 * Encapsulates DDL operations on DStreams.
 *
 * Created by hemantb on 5/13/15.
 */
case class StreamRelation[T](
                              dStream: DStream[T],
                              options: Map[String, Any],
                              formatter: (RDD[T], StructType) => RDD[Row],
                              streamschema: StructType)(
                              @transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with Logging {
  override def schema: StructType = streamschema

  override def buildScan(): RDD[Row] =
    throw new IllegalAccessException("Take it easy boy!! It's a prototype. ")

}

private[sql] class StreamSource extends SchemaRelationProvider {


  def createRelation(sqlContext: SQLContext,
                     options: Map[String, String],
                     schema: StructType): BaseRelation = {

    val addresses: String = OptsUtil.getOption(OptsUtil.SERVER_ADDRESS, options)
    val urls = addresses.split(",").map(addr=> addr.split(":"))
    //val port: Int = OptsUtil.getOption(OptsUtil.PORT, options).toInt

    // Load the format function using reflection
    val formatFunction = loadFormatClass(OptsUtil.getOption(
      OptsUtil.FORMAT, options)).newInstance() match {
      case f: UserDefinedInterpreter[_] =>
        f.asInstanceOf[UserDefinedInterpreter[String]].formatter()_
      case f => throw new AnalysisException(s"Incorrect format function $f")
    }

    // Create a dstream here based on the parameters passed as part of create stream
    val stream = StreamingCtxtHolder.streamingContext.union(
        (urls map { url =>
        StreamingCtxtHolder.streamingContext.
          socketTextStream(url(0), url(1).toInt)
      }).toSeq)

    val dstream: DStream[String] = options.get(OptsUtil.WINDOWDURATION) match {
      case Some(wd) => options.get(OptsUtil.SLIDEDURATION) match {
        case Some(sd) => stream.window(Duration(wd.toInt), Duration(sd.toInt))
        case None => stream.window(Duration(wd.toInt))
      }
      case None => stream
    }

    new StreamRelation(dstream, options, formatFunction, schema)(sqlContext)
  }

  def loadFormatClass(provider: String): Class[_] = {
    val loader = Utils.getContextOrSparkClassLoader
    try {
      loader.loadClass(provider)
    } catch {
      case cnf: java.lang.ClassNotFoundException =>
        sys.error(s"Failed to load class for data source: $provider")
    }
  }
}

object OptsUtil {

  // Options while creating sample/stream table
  val BASETABLE = "basetable"
  val SERVER_ADDRESS = "serveraddress"
  //val PORT = "port"
  val FORMAT = "format"
  val SLIDEDURATION = "slideduration"
  val WINDOWDURATION = "windowduration"

  def getOption(optionName: String, options: Map[String, String]): String =
    options.getOrElse(optionName, throw new AnalysisException(
      s"Option $optionName not defined"))
}

object StreamStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateStream(streamName, userColumns, options) =>
      ExecutedCommand(
        CreateStreamTableCmd(streamName, userColumns, options)) :: Nil
    case CreateSampledTable(streamName, options) =>
      ExecutedCommand(
        CreateSampledTableCmd(streamName, options)) :: Nil
    case StreamingCtxtActions(action, batchInterval) =>
      ExecutedCommand(
        StreamingCtxtActionsCmd(action, batchInterval)) :: Nil
    case _ => Nil
  }
}


private[sql] case class CreateStreamTableCmd(streamName: String,
                                             userColumns: Option[StructType],
                                             options: Map[String, String])
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {

    val resolved = ResolvedDataSource(sqlContext, userColumns,
      Array.empty[String], "org.apache.spark.sql.sources.StreamSource", options)
    val plan = LogicalRelation(resolved.relation)

    // add the stream to the streamtables in the catalog
    sqlContext.asInstanceOf[SnappyContext].catalog.streamTables.put(
      streamName, plan)
    Seq.empty
  }
}

private[sql] case class StreamingCtxtActionsCmd(action: Int,
                                                batchInterval: Option[Int])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {

    action match {
      case 0 =>
        import org.apache.spark.sql.snappy._

        sqlContext.sparkContext.getOrCreateStreamingContext(
          batchInterval.getOrElse(throw new IllegalStateException()))

      case 1 =>
        // Register sampling of all the streams
        val snappyCtxt = sqlContext.asInstanceOf[SnappyContext]
        val catalog = snappyCtxt.catalog
        catalog.streamTables.foreach(streamEntry => {

          val sampleTables = catalog.streamToStructureMap.getOrElse(
            streamEntry._1, Nil)

          val sr = catalog.getStreamTableRelation(streamEntry._1).
            asInstanceOf[StreamRelation[_]]

          // HERE WE ASSUME THAT THE STREAM OF IS OF TYPE STRING.
          // THIS NEEDS TO CHANGE AFTER THE PROTOTYPE
          snappyCtxt.saveStream[String](sr.dStream.asInstanceOf[DStream[String]],
            sampleTables, sr.formatter.asInstanceOf[(RDD[String], StructType) =>
              RDD[Row]], sr.schema)
        })
        // start the streaming
        StreamingCtxtHolder.streamingContext.start()

      case 2 => StreamingCtxtHolder.streamingContext.stop()
    }
    Seq.empty[Row]
  }
}

/**
 * User has to implement this trait to convert the stream RDD[String] to RDD[Row].
 */
trait UserDefinedInterpreter[T] {
  def formatter[R <: RDD[T]]()(r: R, schema: StructType): RDD[Row]
}

private[sql] case class CreateSampledTableCmd(sampledTableName: String,
                                              options: Map[String, String])
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {

    val tableName: String = OptsUtil.getOption(OptsUtil.BASETABLE, options)

    val snappyCtxt = sqlContext.asInstanceOf[SnappyContext]
    val catalog = snappyCtxt.catalog

    val sr = catalog.getStreamTableRelation(tableName)

    //val sampleTables =
    // Add the sample table to the catalog as well.

    catalog.streamToStructureMap.put(tableName,
      catalog.streamToStructureMap.getOrElse(tableName, Nil) :+ sampledTableName)
    // Register the sample table
    // StratifiedSampler is not expecting basetable, remove it.
    snappyCtxt.registerSampleTable(sampledTableName, sr.schema,
      options - OptsUtil.BASETABLE)

    Seq.empty
  }
}