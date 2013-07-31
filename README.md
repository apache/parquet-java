Parquet MR [![Build Status](https://travis-ci.org/Parquet/parquet-mr.png?branch=master)](http://travis-ci.org/Parquet/parquet-mr)
======

Parquet-MR contains the java implementation of the [Parquet format](https://github.com/Parquet/parquet-format). 
Parquet is a columnar storage format for Hadoop; it provides efficient storage and encoding of data.
Parquet uses the [record shredding and assembly algorithm](https://github.com/Parquet/parquet-mr/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) described in the Dremel paper to represent nested structures.

You can find some details about the format and intended use cases in our [Hadoop Summit 2013 presentation](http://www.slideshare.net/julienledem/parquet-hadoop-summit-2013)

## Features

Parquet is a very active project, and new features are being added quickly; below is the state as of June 2013.


<table>
  <tr><th>Feature</th><th>In trunk</th><th>In dev</th><th>Planned</th><th>Expected release</th></tr>
  <tr><td>Type-specific encoding</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Hive integration</td><td>YES (<a href ="https://github.com/Parquet/parquet-mr/pull/28">28</a>)</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Pig integration</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Cascading integration</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Impala integration</td><td>YES (non-nested)</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Java Map/Reduce API</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Native Avro support</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Native Thrift support</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Complex structure support</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Future-proofed versioning</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>RLE</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Bit Packing</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Adaptive dictionary encoding</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Complex structure support</td><td>YES</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Predicate pushdown</td><td>YES (<a href ="https://github.com/Parquet/parquet-mr/pull/68">68</a>)</td><td></td></td><td></td><td>1.0</td></tr>
  <tr><td>Column stats</td><td></td><td></td></td><td>YES</td><td>2.0</td></tr>  <tr><td>Delta encoding</td><td></td><td></td></td><td>YES</td><td>2.0</td></tr>
  <tr><td>Native Protocol Buffers support</td><td></td><td></td></td><td>YES</td><td>2.0</td></tr>
  <tr><td>Index pages</td><td></td><td></td></td><td>YES</td><td>2.0</td></tr>
</table>

## Map/Reduce integration

[Input](https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetInputFormat.java) and [Output](https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetOutputFormat.java) formats.
Note that to use an Input or Output format, you need to implement a WriteSupport or ReadSupport class, which will implement the conversion of your object to and from a Parquet schema.

We've implemented this for 2 popular data formats to provide a clean migration path as well:

### Thrift
Thrift integration is provided by the [parquet-thrift](https://github.com/Parquet/parquet-mr/tree/master/parquet-thrift) sub-project. If you are using Thrift through Scala, you may be using Twitter's [Scrooge](https://github.com/twitter/scrooge). If that's the case, not to worry -- we took care of the Scrooge/Apache Thrift glue for you in the [parquet-scrooge](https://github.com/Parquet/parquet-mr/tree/master/parquet-scrooge) sub-project.

### Avro
Avro conversion is implemented via the [parquet-avro](https://github.com/Parquet/parquet-mr/tree/master/parquet-avro) sub-project.

### Create your own objects
* The ParquetOutputFormat can be provided a WriteSupport to write your own objects to an event based RecordConsumer.
* the ParquetInputFormat can be provided a ReadSupport to materialize your own objects by implementing a RecordMaterializer

See the APIs:
* [Record conversion API](https://github.com/Parquet/parquet-mr/tree/master/parquet-column/src/main/java/parquet/io/api)
* [Hadoop API](https://github.com/Parquet/parquet-mr/tree/master/parquet-hadoop/src/main/java/parquet/hadoop/api)

## Apache Pig integration
A [Loader](https://github.com/Parquet/parquet-mr/blob/master/parquet-pig/src/main/java/parquet/pig/ParquetLoader.java) and a [Storer](https://github.com/Parquet/parquet-mr/blob/master/parquet-pig/src/main/java/parquet/pig/ParquetStorer.java) are provided to read and write Parquet files with Apache Pig

Storing data into Parquet in Pig is simple:
```
-- options you might want to fiddle with
SET parquet.page.size 1048576 -- default. this is your min read/write unit.
SET parquet.block.size 524288000 -- your memory budget for buffering data
SET parquet.compression lzo -- or you can use none, gzip, snappy
STORE mydata into '/some/path' USING parquet.pig.ParquetStorer;
```
Reading in Pig is also simple:
```
mydata = LOAD '/some/path' USING parquet.pig.ParquetLoader();
```

If the data was stored using Pig, things will "just work". If the data was stored using another method, you will need to provide the Pig schema equivalent to the data you stored (you can also write the schema to the file footer while writing it -- but that's pretty advanced). We will provide a basic automatic schema conversion soon.

## Hive integration

Hive integration is provided via the [parquet-hive](https://github.com/Parquet/parquet-mr/tree/master/parquet-hive) sub-project.

## Build

to run the unit tests:
mvn test

to build the jars:
mvn package

The build runs in [Travis CI](http://travis-ci.org/Parquet/parquet-mr):
[![Build Status](https://secure.travis-ci.org/Parquet/parquet-mr.png?branch=master)](http://travis-ci.org/Parquet/parquet-mr)

## Add Parquet as a dependency in Maven

### Snapshot releases
* [apis documentation](http://parquet.io/parquet-mr/site/1.0.0-SNAPSHOT/apidocs/index.html)
* maven dependency:

```xml
  <repositories>
    <repository>
      <id>sonatype-nexus-snapshots</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots</url>
      <releases>
        <enabled>false</enabled>
      </releases>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
     </repository>
  </repositories>
  <dependencies>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>parquet-common</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>parquet-encoding</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>parquet-column</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>parquet-hadoop</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

### Official releases
#### 1.0.0
* [apis documentation](http://parquet.io/parquet-mr/site/1.0.0/apidocs/index.html)
* maven dependency:

```xml
  <dependencies>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>parquet-common</artifactId>
      <version>1.0.0</version>
    </dependency>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>parquet-encoding</artifactId>
      <version>1.0.0</version>
    </dependency>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>parquet-column</artifactId>
      <version>1.0.0</version>
    </dependency>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>parquet-hadoop</artifactId>
      <version>1.0.0</version>
    </dependency>
  </dependencies>
```

### How To Contribute

If you are looking for some ideas on what to contribute, check out GitHub issues for this project labeled ["Pick me up!"](https://github.com/Parquet/parquet-mr/issues?labels=pick+me+up%21&state=open).
Comment on the issue and/or contact [the parquet-dev group](https://groups.google.com/d/forum/parquet-dev) with your questions and ideas.

We tend to do fairly close readings of pull requests, and you may get a lot of comments. Some common issues that are not code structure related, but still important:
* Please make sure to add the license headers to all new files. You can do this automatically by using the `mvn license:format` command.
* Use 2 spaces for whitespace. Not tabs, not 4 spaces. The number of the spacing shall be 2.
* Give your operators some room. Not `a+b` but `a + b` and not `foo(int a,int b)` but `foo(int a, int b)`.
* Generally speaking, stick to the [Sun Java Code Conventions](http://www.oracle.com/technetwork/java/javase/documentation/codeconvtoc-136057.html)
* Make sure tests pass!

## Authors and contributors

* Julien Le Dem [@J_](http://twitter.com/J_) <https://github.com/julienledem>
* Tom White <https://github.com/tomwhite>
* Mickaël Lacour <https://github.com/mickaellcr>
* Remy Pecqueur <https://github.com/Lordshinjo>
* Avi Bryant <https://github.com/avibryant>
* Dmitriy Ryaboy [@squarecog](https://twitter.com/squarecog) <https://github.com/dvryaboy>
* Jonathan Coveney <http://twitter.com/jco>
* and many others -- see the [Contributor report]( https://github.com/Parquet/parquet-mr/contributors)

## Discussions
* google group https://groups.google.com/d/forum/parquet-dev
* the group email address: parquet-dev@googlegroups.com

## License

Copyright 2012-2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

