Parquet MR [![Build Status](https://travis-ci.org/Parquet/parquet-mr.png?branch=master)](http://travis-ci.org/Parquet/parquet-mr)
======

Parquet-mr is the java implementation of the [Parquet format](https://github.com/Parquet/parquet-format) to be used in Hadoop. 
It uses the [record shredding and assembly algorithm](https://github.com/Parquet/parquet-mr/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) described in the Dremel paper.
Integration with Pig and Map/Reduce are provided.

## Apache Pig integration
A [Loader](https://github.com/Parquet/parquet-mr/blob/master/parquet-pig/src/main/java/parquet/pig/ParquetLoader.java) and a [Storer](https://github.com/Parquet/parquet-mr/blob/master/parquet-pig/src/main/java/parquet/pig/ParquetStorer.java) are provided to read and write Parquet files with Apache Pig

## Map/Reduce integration

### Thrift
Thrift mapping to the parquet schema is provided using a TBase extending class.
You can read and write parquet files using Thrift generated classes.

### Create your own objects
* The ParquetOutputFormat can be provided a WriteSupport to write your own objects to an event based RecordConsumer.
* the ParquetInputFormat can be provided a ReadSupport to materialize your own POJOs by implementing a RecordMaterializer

See the APIs:
* [Record convertion API](https://github.com/Parquet/parquet-mr/tree/master/parquet-column/src/main/java/parquet/io/api)
* [Hadoop API](https://github.com/Parquet/parquet-mr/tree/master/parquet-hadoop/src/main/java/parquet/hadoop/api)

## Build

to run the unit tests:
mvn test

to build the jars:
mvn package

The build runs in [Travis CI](http://travis-ci.org/Parquet/parquet-mr):
[![Build Status](https://secure.travis-ci.org/Parquet/parquet-mr.png?branch=master)](http://travis-ci.org/Parquet/parquet-mr)

## Authors and contributors

* Julien Le Dem <http://twitter.com/J_>
* Jonathan Coveney <http://twitter.com/jco>

## Discussions
* google group https://groups.google.com/d/forum/parquet-dev
* the group email address: parquet-dev@googlegroups.com

## License

Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

