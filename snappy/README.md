## Building snappy-spark with Scala 2.11.x

The latest version of Scala available at the time of writing this was 2.11.6.
Unfortunately spark does not build with that version out of the box.
The snappy-spark repository now has the required additions checked in.

Use this to get a full build with Hive/Thrift support:

build/sbt -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.1 -Phive -Phive-thriftserver -Pyarn -Dscala-2.11 -DskipTests assembly

Use the spark-build.sh script in snappy directory of this repo which has
these arguments by default. Any arguments to the script will be passed
through to sbt/mvn except for these which are handled by the script:

    -scala-2.11: to build with scala-2.11
    -scala-2.10: to build with scala-2.10 (default)
    -mvn: to build with maven instead of sbt

You need to have maven installed on the system to build with maven but
no need for an sbt installation since the build/sbt script in spark pulls
everything as required.


## Running Spark shell

For interactive usage, try bin/spark-shell. The spark-shell-launch.sh script
in the scripts directory of this repository will start a local node with
6 threads and 3G of memory by default. Any arguments passed to the
script will override these defaults.

A basic test is checked in tests/src/main/scala/io/snappydata/SparkSQLTest.scala
in the SnappySparkTools directory of the experiments repository.
You can just copy the contents of file to spark-shell. Now it also has an sbt
build configuration that will automatically pull the latest snappy-spark
SNAPSHOT build. The data file referred in that test is 2007 and 2008 airline
data mentioned in snappy resources with the first header line removed. This
file can also be found as SnappyData/data/2007-8.csv.bz2 in Google drive.
Note that it is compressed with bzip2 which can be uncompressed on most
OSes with builtin tools (e.g. bunzip2 on Linux)
