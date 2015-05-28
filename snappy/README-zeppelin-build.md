The following steps are tested working to build Zeppelin master against snappy-spark.

1. Build snappy-spark with scala-2.10. Required because Zepplin seems to be having trouble building with 2.11 version of snappy-spark-repl library. Use this with our build script in snappy-spark repository:

    snappy/spark-build.sh -scala-2.10 compile publish-local

2. Apply the patch in this directory to zeppelin sources (with "patch -p1 < zeppelin-snappy.diff").  This will change artifacts to snappy-spark-* from spark-* as created by snappy-spark builds.

3. Lastly build zeppelin with:

    mvn -DskipTests -Dspark.version=1.4.0-SNAPSHOT.2 -Dhadoop.version=2.4.1 -Pspark-1.4 install


Note: When building zeppelin, if you get errors that it can't find snappy
jars like:

[ERROR] Failed to execute goal on project zeppelin-spark: Could not resolve
        dependencies for project
        org.apache.zeppelin:zeppelin-spark:jar:0.5.0-incubating-SNAPSHOT:
        The following artifacts could not be resolved:
          org.apache.spark:snappy-spark-launcher_2.10:jar:1.4.0-SNAPSHOT.2,
          org.apache.spark:snappy-spark-unsafe_2.10:jar:1.4.0-SNAPSHOT.2:
        Could not find artifact
        org.apache.spark:snappy-spark-launcher_2.10:jar:1.4.0-SNAPSHOT.2 in central
        (https://repo.maven.apache.org/maven2) -> [Help 1]

This was because the javadoc targets failed due to <p/> in *comments* etc.
I couldn't figure out how to turn off javadoc or ignore javadoc errors
(-Dmaven.javadoc.skip=true did not work, nor did putting the property in
the appropriate pom.xml...)  So, I fixed the unsafe and launcher comments
in snappy-spark/master    If new errors pop up in the future, then look to
javadoc errors in the snappy-spark build, since those jars won't be
available via publish-local.
