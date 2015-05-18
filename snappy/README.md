## Git configuration to use keyring/keychain

Snappy is currently hosting private repositories and will continue to do
so for foreseable future. It is possible to configure git to enable
using gnome-keyring on Linux platforms, and KeyChain on OSX
(sumedh: latter not verified by me yet, so someone who uses OSX should do it)

On Linux Ubuntu/Mint:

Install gnome-keyring dev files: sudo aptitude install libgnome-keyring-dev

Build git-credential-gnome-keyring:

    cd /usr/share/doc/git/contrib/credential/gnome-keyring
    sudo make

Copy to PATH (optional):

    sudo cp git-credential-gnome-keyring /usr/local/bin
    sudo make clean

Note that if you skip this step then need to give full path in the next
step i.e. /usr/share/doc/git/contrib/credential/gnome-keyring/git-credential-gnome-keyring

Configure git: git config --global credential.helper gnome-keyring

Similarly on OSX locate git-credential-osxkeychain, build it if not present
(it is named "osxkeychain" instead of gnome-keyring), then set in git config

Now your git password will be stored in keyring/keychain which is normally
unlocked automatically on login (or you will be asked to unlock on first use).

On Linux, you can install "seahorse", if not already, to see/modify all
the passwords in keyring (GUI menu "Passwords and Keys" under Preferences
or Accessories or System Tools)


## Building snappy-spark with Scala 2.11.x

The latest version of Scala available at the time of writing this was 2.11.6.
Unfortunately spark does not build with that version out of the box.
The snappy-spark repository now has the required additions checked in.

Use this to get a full build with Hive/Thrift support:

build/sbt -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.1 -Phive -Phive-thriftserver -Dscala-2.11 -DskipTests assembly

Use the spark-build.sh script in scripts directory of this repo which has
these arguments by default. Any arguments to the script will be passed
through to sbt/mvn except for these which are handled by the script:

    -scala-2.11: to build with scala-2.11 (default with sbt)
    -scala-2.10: to build with scala-2.10 (default with mvn)
    -mvn: to build with maven instead of sbt

You need to have maven installed on the system to build with maven but
no need for an sbt installation since the build/sbt script in spark pulls
everything as required.


## Eclipse with snappy-spark

The easiest way is to get the scala IDE from http://scala-ide.org which is
eclipse with scala plugin preconfigured. Latest version 4.0.0 at the time of
writing works quite well overall.

First generate the eclipse files using sbt's eclipse target e.g.

build/sbt -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.1 -Phive -Phive-thriftserver -Dscala-2.11 -DskipTests eclipse

OR

spark-build.sh eclipse


To add references to source and javadoc jars, use:

build/sbt -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.1 -Phive -Phive-thriftserver -Dscala-2.11 -DskipTests "eclipse with-source=true"

OR

spark-build.sh "eclipse with-source=true"

Next import the eclipse projects preferably in a fresh workspace
"File->Import->General->Existing Projects into Workspace". For a start,
import the following (preferably in that order):

    spark-launcher           from launcher directory in the checkout
    spark-network-common     from network/common
    spark-network-shuffle    from network/shuffle
    spark-core               from core
    spark-catalyst           from sql/catalyst
    spark-sql                from sql/core
    spark-hive               from sql/hive
    spark-streaming          from streaming
    spark-hive-thriftserver  from sql/hive-thriftserver

These should be enough for current changes to spark we plan on doing.

Alternatively untar the eclipse-files.tgz inside the checkout which will
overwrite the .classpath with these for the above mentioned 9 projects.


## Running Spark SQL

For interactive usage, try bin/spark-shell. The spark-shell-launch.sh script
in the scripts directory of this repository will start a local node with
6 threads and 3G of memory by default. Any arguments passed to the
script will override these defaults.

A basic test is checked in tests/src/main/scala/io/snappydata/SparkSQLTest.scala.
You can just copy the contents of file to spark-shell. Now it also has an sbt
build configuration that will automatically pull the latest snappy-spark
SNAPSHOT build. The data file referred in that test is 2007 and 2008 airline
data mentioned in snappy resources with the first header line removed. This
file can also be found as SnappyData/data/2007-8.csv.bz2 in Google drive.
Note that it is compressed with bzip2 which can be uncompressed on most
OSes with builtin tools (e.g. bunzip2 on Linux)
