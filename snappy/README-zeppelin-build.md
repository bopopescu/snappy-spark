The following steps are tested working to build Zeppelin master against snappy-spark.

1. Build snappy-spark with scala-2.10. Required because Zepplin seems to be having trouble building with 2.11 version of snappy-spark-repl library. Use this with our build script in snappy-spark repository:

    snappy/spark-build.sh -scala-2.10 compile publish-local

2. Apply the patch in this directory to zeppelin sources (with "patch -p1 < zeppelin-snappy.diff").  This will change artifacts to snappy-spark-* from spark-* as created by snappy-spark builds.

3. Lastly build zeppelin with:

    mvn -DskipTests -Dspark.version=1.4.0-SNAPSHOT.2 -Dhadoop.version=2.4.1 -Pspark-1.4 install


