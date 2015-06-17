<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Parquet #

### Version 1.7.0 ###

*   [PARQUET-23](https://issues.apache.org/jira/browse/PARQUET-23) - Rename to org.apache.

### Version 1.6.0 ###

####  Bug

*   [PARQUET-3](https://issues.apache.org/jira/browse/PARQUET-3) - tool to merge pull requests based on Spark
*   [PARQUET-4](https://issues.apache.org/jira/browse/PARQUET-4) - Use LRU caching for footers in ParquetInputFormat.
*   [PARQUET-8](https://issues.apache.org/jira/browse/PARQUET-8) - [parquet-scrooge] mvn eclipse:eclipse fails on parquet-scrooge
*   [PARQUET-9](https://issues.apache.org/jira/browse/PARQUET-9) - InternalParquetRecordReader will not read multiple blocks when filtering
*   [PARQUET-18](https://issues.apache.org/jira/browse/PARQUET-18) - Cannot read dictionary-encoded pages with all null values
*   [PARQUET-19](https://issues.apache.org/jira/browse/PARQUET-19) - NPE when an empty file is included in a Hive query that uses CombineHiveInputFormat
*   [PARQUET-21](https://issues.apache.org/jira/browse/PARQUET-21) - Fix reference to 'github-apache' in dev docs
*   [PARQUET-56](https://issues.apache.org/jira/browse/PARQUET-56) - Added an accessor for the Long column type in example Group
*   [PARQUET-62](https://issues.apache.org/jira/browse/PARQUET-62) - DictionaryValuesWriter dictionaries are corrupted by user changes.
*   [PARQUET-63](https://issues.apache.org/jira/browse/PARQUET-63) - Fixed-length columns cannot be dictionary encoded.
*   [PARQUET-66](https://issues.apache.org/jira/browse/PARQUET-66) - InternalParquetRecordWriter int overflow causes unnecessary memory check warning
*   [PARQUET-69](https://issues.apache.org/jira/browse/PARQUET-69) - Add committer doc and REVIEWERS files
*   [PARQUET-70](https://issues.apache.org/jira/browse/PARQUET-70) - PARQUET #36: Pig Schema Storage to UDFContext
*   [PARQUET-75](https://issues.apache.org/jira/browse/PARQUET-75) - String decode using 'new String' is slow
*   [PARQUET-80](https://issues.apache.org/jira/browse/PARQUET-80) - upgrade semver plugin version to 0.9.27
*   [PARQUET-82](https://issues.apache.org/jira/browse/PARQUET-82) - ColumnChunkPageWriteStore assumes pages are smaller than Integer.MAX\_VALUE
*   [PARQUET-88](https://issues.apache.org/jira/browse/PARQUET-88) - Fix pre-version enforcement.
*   [PARQUET-94](https://issues.apache.org/jira/browse/PARQUET-94) - ParquetScroogeScheme constructor ignores klass argument
*   [PARQUET-96](https://issues.apache.org/jira/browse/PARQUET-96) - parquet.example.data.Group is missing some methods
*   [PARQUET-97](https://issues.apache.org/jira/browse/PARQUET-97) - ProtoParquetReader builder factory method not static
*   [PARQUET-101](https://issues.apache.org/jira/browse/PARQUET-101) - Exception when reading data with parquet.task.side.metadata=false
*   [PARQUET-104](https://issues.apache.org/jira/browse/PARQUET-104) - Parquet writes empty Rowgroup at the end of the file
*   [PARQUET-106](https://issues.apache.org/jira/browse/PARQUET-106) - Relax InputSplit Protections
*   [PARQUET-107](https://issues.apache.org/jira/browse/PARQUET-107) - Add option to disable summary metadata aggregation after MR jobs
*   [PARQUET-114](https://issues.apache.org/jira/browse/PARQUET-114) - Sample NanoTime class serializes and deserializes Timestamp incorrectly
*   [PARQUET-122](https://issues.apache.org/jira/browse/PARQUET-122) - make parquet.task.side.metadata=true by default
*   [PARQUET-124](https://issues.apache.org/jira/browse/PARQUET-124) - parquet.hadoop.ParquetOutputCommitter.commitJob() throws parquet.io.ParquetEncodingException
*   [PARQUET-132](https://issues.apache.org/jira/browse/PARQUET-132) - AvroParquetInputFormat should use a parameterized type
*   [PARQUET-135](https://issues.apache.org/jira/browse/PARQUET-135) - Input location is not getting set for the getStatistics in ParquetLoader when using two different loaders within a Pig script.
*   [PARQUET-136](https://issues.apache.org/jira/browse/PARQUET-136) - NPE thrown in StatisticsFilter when all values in a string/binary column trunk are null
*   [PARQUET-142](https://issues.apache.org/jira/browse/PARQUET-142) - parquet-tools doesn't filter \_SUCCESS file
*   [PARQUET-145](https://issues.apache.org/jira/browse/PARQUET-145) - InternalParquetRecordReader.close() should not throw an exception if initialization has failed
*   [PARQUET-150](https://issues.apache.org/jira/browse/PARQUET-150) - Merge script requires ':' in PR names
*   [PARQUET-157](https://issues.apache.org/jira/browse/PARQUET-157) - Divide by zero in logging code
*   [PARQUET-159](https://issues.apache.org/jira/browse/PARQUET-159) - paquet-hadoop tests fail to compile
*   [PARQUET-162](https://issues.apache.org/jira/browse/PARQUET-162) - ParquetThrift should throw when unrecognized columns are passed to the column projection API
*   [PARQUET-168](https://issues.apache.org/jira/browse/PARQUET-168) - Wrong command line option description in parquet-tools
*   [PARQUET-173](https://issues.apache.org/jira/browse/PARQUET-173) - StatisticsFilter doesn't handle And properly
*   [PARQUET-174](https://issues.apache.org/jira/browse/PARQUET-174) - Fix Java6 compatibility
*   [PARQUET-176](https://issues.apache.org/jira/browse/PARQUET-176) - Parquet fails to parse schema contains '\r'
*   [PARQUET-180](https://issues.apache.org/jira/browse/PARQUET-180) - Parquet-thrift compile issue with 0.9.2.
*   [PARQUET-184](https://issues.apache.org/jira/browse/PARQUET-184) - Add release scripts and documentation
*   [PARQUET-186](https://issues.apache.org/jira/browse/PARQUET-186) - Poor performance in SnappyCodec because of string concat in tight loop
*   [PARQUET-187](https://issues.apache.org/jira/browse/PARQUET-187) - parquet-scrooge doesn't compile under 2.11
*   [PARQUET-188](https://issues.apache.org/jira/browse/PARQUET-188) - Parquet writes columns out of order (compared to the schema)
*   [PARQUET-189](https://issues.apache.org/jira/browse/PARQUET-189) - Support building parquet with thrift 0.9.0
*   [PARQUET-196](https://issues.apache.org/jira/browse/PARQUET-196) - parquet-tools command to get rowcount & size
*   [PARQUET-197](https://issues.apache.org/jira/browse/PARQUET-197) - parquet-cascading and the mapred API does not create metadata file
*   [PARQUET-202](https://issues.apache.org/jira/browse/PARQUET-202) - Typo in the connection info in the pom prevents publishing an RC
*   [PARQUET-207](https://issues.apache.org/jira/browse/PARQUET-207) - ParquetInputSplit end calculation bug
*   [PARQUET-208](https://issues.apache.org/jira/browse/PARQUET-208) - revert PARQUET-197
*   [PARQUET-214](https://issues.apache.org/jira/browse/PARQUET-214) - Avro: Regression caused by schema handling
*   [PARQUET-215](https://issues.apache.org/jira/browse/PARQUET-215) - Parquet Thrift should discard records with unrecognized union members
*   [PARQUET-216](https://issues.apache.org/jira/browse/PARQUET-216) - Decrease the default page size to 64k
*   [PARQUET-217](https://issues.apache.org/jira/browse/PARQUET-217) - Memory Manager's min allocation heuristic is not valid for schemas with many columns
*   [PARQUET-232](https://issues.apache.org/jira/browse/PARQUET-232) - minor compilation issue
*   [PARQUET-234](https://issues.apache.org/jira/browse/PARQUET-234) - Restore ParquetInputSplit methods from 1.5.0
*   [PARQUET-235](https://issues.apache.org/jira/browse/PARQUET-235) - Fix compatibility of parquet.metadata with 1.5.0
*   [PARQUET-236](https://issues.apache.org/jira/browse/PARQUET-236) - Check parquet-scrooge compatibility
*   [PARQUET-237](https://issues.apache.org/jira/browse/PARQUET-237) - Check ParquetWriter constructor compatibility with 1.5.0
*   [PARQUET-239](https://issues.apache.org/jira/browse/PARQUET-239) - Make AvroParquetReader#builder() static
*   [PARQUET-242](https://issues.apache.org/jira/browse/PARQUET-242) - AvroReadSupport.setAvroDataSupplier is broken

####  Improvement

*   [PARQUET-2](https://issues.apache.org/jira/browse/PARQUET-2) - Adding Type Persuasion for Primitive Types
*   [PARQUET-25](https://issues.apache.org/jira/browse/PARQUET-25) - Pushdown predicates only work with hardcoded arguments
*   [PARQUET-52](https://issues.apache.org/jira/browse/PARQUET-52) - Improve the encoding fall back mechanism for Parquet 2.0
*   [PARQUET-57](https://issues.apache.org/jira/browse/PARQUET-57) - Make dev commit script easier to use
*   [PARQUET-61](https://issues.apache.org/jira/browse/PARQUET-61) - Avoid fixing protocol events when there is not required field missing
*   [PARQUET-74](https://issues.apache.org/jira/browse/PARQUET-74) - Use thread local decoder cache in Binary toStringUsingUTF8()
*   [PARQUET-79](https://issues.apache.org/jira/browse/PARQUET-79) - Add thrift streaming API to read metadata
*   [PARQUET-84](https://issues.apache.org/jira/browse/PARQUET-84) - Add an option to read the rowgroup metadata on the task side.
*   [PARQUET-87](https://issues.apache.org/jira/browse/PARQUET-87) - Better and unified API for projection pushdown on cascading scheme
*   [PARQUET-89](https://issues.apache.org/jira/browse/PARQUET-89) - All Parquet CI tests should be run against hadoop-2
*   [PARQUET-92](https://issues.apache.org/jira/browse/PARQUET-92) - Parallel Footer Read Control
*   [PARQUET-105](https://issues.apache.org/jira/browse/PARQUET-105) - Refactor and Document Parquet Tools
*   [PARQUET-108](https://issues.apache.org/jira/browse/PARQUET-108) - Parquet Memory Management in Java
*   [PARQUET-115](https://issues.apache.org/jira/browse/PARQUET-115) - Pass a filter object to user defined predicate in filter2 api
*   [PARQUET-116](https://issues.apache.org/jira/browse/PARQUET-116) - Pass a filter object to user defined predicate in filter2 api
*   [PARQUET-117](https://issues.apache.org/jira/browse/PARQUET-117) - implement the new page format for Parquet 2.0
*   [PARQUET-119](https://issues.apache.org/jira/browse/PARQUET-119) - add data\_encodings to ColumnMetaData to enable dictionary based predicate push down
*   [PARQUET-121](https://issues.apache.org/jira/browse/PARQUET-121) - Allow Parquet to build with Java 8
*   [PARQUET-128](https://issues.apache.org/jira/browse/PARQUET-128) - Optimize the parquet RecordReader implementation when: A. filterpredicate is pushed down , B. filterpredicate is pushed down on a flat schema
*   [PARQUET-133](https://issues.apache.org/jira/browse/PARQUET-133) - Upgrade snappy-java to 1.1.1.6
*   [PARQUET-134](https://issues.apache.org/jira/browse/PARQUET-134) - Enhance ParquetWriter with file creation flag
*   [PARQUET-140](https://issues.apache.org/jira/browse/PARQUET-140) - Allow clients to control the GenericData object that is used to read Avro records
*   [PARQUET-141](https://issues.apache.org/jira/browse/PARQUET-141) - improve parquet scrooge integration
*   [PARQUET-160](https://issues.apache.org/jira/browse/PARQUET-160) - Simplify CapacityByteArrayOutputStream
*   [PARQUET-165](https://issues.apache.org/jira/browse/PARQUET-165) - A benchmark module for Parquet would be nice
*   [PARQUET-177](https://issues.apache.org/jira/browse/PARQUET-177) - MemoryManager ensure minimum Column Chunk size
*   [PARQUET-181](https://issues.apache.org/jira/browse/PARQUET-181) - Scrooge Write Support
*   [PARQUET-191](https://issues.apache.org/jira/browse/PARQUET-191) - Avro schema conversion incorrectly converts maps with nullable values.
*   [PARQUET-192](https://issues.apache.org/jira/browse/PARQUET-192) - Avro maps drop null values
*   [PARQUET-193](https://issues.apache.org/jira/browse/PARQUET-193) - Avro: Implement read compatibility rules for nested types
*   [PARQUET-203](https://issues.apache.org/jira/browse/PARQUET-203) - Consolidate PathFilter for hidden files
*   [PARQUET-204](https://issues.apache.org/jira/browse/PARQUET-204) - Directory support for parquet-schema
*   [PARQUET-210](https://issues.apache.org/jira/browse/PARQUET-210) - JSON output for parquet-cat

####  New Feature

*   [PARQUET-22](https://issues.apache.org/jira/browse/PARQUET-22) - Parquet #13: Backport of HIVE-6938
*   [PARQUET-49](https://issues.apache.org/jira/browse/PARQUET-49) - Create a new filter API that supports filtering groups of records based on their statistics
*   [PARQUET-64](https://issues.apache.org/jira/browse/PARQUET-64) - Add new logical types to parquet-column
*   [PARQUET-123](https://issues.apache.org/jira/browse/PARQUET-123) - Add dictionary support to AvroIndexedRecordReader
*   [PARQUET-198](https://issues.apache.org/jira/browse/PARQUET-198) - parquet-cascading Add Parquet Avro Scheme

####  Task

*   [PARQUET-50](https://issues.apache.org/jira/browse/PARQUET-50) - Remove items from semver blacklist
*   [PARQUET-139](https://issues.apache.org/jira/browse/PARQUET-139) - Avoid reading file footers in parquet-avro InputFormat
*   [PARQUET-190](https://issues.apache.org/jira/browse/PARQUET-190) - Fix an inconsistent Javadoc comment of ReadSupport.prepareForRead
*   [PARQUET-230](https://issues.apache.org/jira/browse/PARQUET-230) - Add build instructions to the README

### Version 1.5.0 ###
* ISSUE [399](https://github.com/Parquet/parquet-mr/pull/399): Fixed resetting stats after writePage bug, unit testing of readFooter
* ISSUE [397](https://github.com/Parquet/parquet-mr/pull/397): Fixed issue with column pruning when using requested schema
* ISSUE [389](https://github.com/Parquet/parquet-mr/pull/389): Added padding for requested columns not found in file schema
* ISSUE [392](https://github.com/Parquet/parquet-mr/pull/392): Value stats fixes
* ISSUE [338](https://github.com/Parquet/parquet-mr/pull/338): Added statistics to Parquet pages and rowGroups
* ISSUE [351](https://github.com/Parquet/parquet-mr/pull/351): Fix bug #350, fixed length argument out of order.
* ISSUE [378](https://github.com/Parquet/parquet-mr/pull/378): configure semver to enforce semantic versioning
* ISSUE [355](https://github.com/Parquet/parquet-mr/pull/355): Add support for DECIMAL type annotation.
* ISSUE [336](https://github.com/Parquet/parquet-mr/pull/336): protobuf dependency version changed from 2.4.1 to 2.5.0
* ISSUE [337](https://github.com/Parquet/parquet-mr/pull/337): issue #324, move ParquetStringInspector to org.apache.hadoop.hive.serde...

### Version 1.4.3 ###
* ISSUE [381](https://github.com/Parquet/parquet-mr/pull/381): fix metadata concurency problem

### Version 1.4.2 ###
* ISSUE [359](https://github.com/Parquet/parquet-mr/pull/359): Expose values in SimpleRecord
* ISSUE [335](https://github.com/Parquet/parquet-mr/pull/335): issue #290, hive map conversion to parquet schema
* ISSUE [365](https://github.com/Parquet/parquet-mr/pull/365): generate splits by min max size, and align to HDFS block when possible
* ISSUE [353](https://github.com/Parquet/parquet-mr/pull/353): Fix bug: optional enum field causing ScroogeSchemaConverter to fail
* ISSUE [362](https://github.com/Parquet/parquet-mr/pull/362): Fix output bug during parquet-dump command
* ISSUE [366](https://github.com/Parquet/parquet-mr/pull/366): do not call schema converter to generate projected schema when projection is not set
* ISSUE [367](https://github.com/Parquet/parquet-mr/pull/367): make ParquetFileWriter throw IOException in invalid state case
* ISSUE [352](https://github.com/Parquet/parquet-mr/pull/352): Parquet thrift storer
* ISSUE [349](https://github.com/Parquet/parquet-mr/pull/349): fix header bug

### Version 1.4.1 ###
* ISSUE [344](https://github.com/Parquet/parquet-mr/pull/344): select * from parquet hive table containing map columns runs into exception. Issue #341.
* ISSUE [347](https://github.com/Parquet/parquet-mr/pull/347): set reading length in ThriftBytesWriteSupport to avoid potential OOM cau...
* ISSUE [346](https://github.com/Parquet/parquet-mr/pull/346): stop using strings and b64 for compressed input splits
* ISSUE [345](https://github.com/Parquet/parquet-mr/pull/345): set cascading version to 2.5.3
* ISSUE [342](https://github.com/Parquet/parquet-mr/pull/342): compress kv pairs in ParquetInputSplits
 
### Version 1.4.0 ###
* ISSUE [333](https://github.com/Parquet/parquet-mr/pull/333): Compress schemas in split
* ISSUE [329](https://github.com/Parquet/parquet-mr/pull/329): fix filesystem resolution
* ISSUE [320](https://github.com/Parquet/parquet-mr/pull/320): Spelling fix
* ISSUE [319](https://github.com/Parquet/parquet-mr/pull/319): oauth based authentication; fix grep change
* ISSUE [310](https://github.com/Parquet/parquet-mr/pull/310): Merge parquet tools
* ISSUE [314](https://github.com/Parquet/parquet-mr/pull/314): Fix avro schema conv for arrays of optional type for #312.
* ISSUE [311](https://github.com/Parquet/parquet-mr/pull/311): Avro null default values bug
* ISSUE [316](https://github.com/Parquet/parquet-mr/pull/316): Update poms to use thrift.exectuable property.
* ISSUE [285](https://github.com/Parquet/parquet-mr/pull/285): [CASCADING] Provide the sink implementation for ParquetTupleScheme
* ISSUE [264](https://github.com/Parquet/parquet-mr/pull/264): Native Protocol Buffer support
* ISSUE [293](https://github.com/Parquet/parquet-mr/pull/293): Int96 support
* ISSUE [313](https://github.com/Parquet/parquet-mr/pull/313): Add hadoop Configuration to Avro and Thrift writers (#295).
* ISSUE [262](https://github.com/Parquet/parquet-mr/pull/262): Scrooge schema converter and projection pushdown in Scrooge
* ISSUE [297](https://github.com/Parquet/parquet-mr/pull/297): Ports HIVE-5783 to the parquet-hive module
* ISSUE [303](https://github.com/Parquet/parquet-mr/pull/303): Avro read schema aliases
* ISSUE [299](https://github.com/Parquet/parquet-mr/pull/299): Fill in default values for new fields in the Avro read schema
* ISSUE [298](https://github.com/Parquet/parquet-mr/pull/298): Bugfix reorder thrift fields causing writting nulls
* ISSUE [289](https://github.com/Parquet/parquet-mr/pull/289): first use current thread's classloader to load a class, if current threa...
* ISSUE [292](https://github.com/Parquet/parquet-mr/pull/292): Added ParquetWriter() that takes an instance of Hadoop's Configuration.
* ISSUE [282](https://github.com/Parquet/parquet-mr/pull/282): Avro default read schema
* ISSUE [280](https://github.com/Parquet/parquet-mr/pull/280): style: junit.framework to org.junit
* ISSUE [270](https://github.com/Parquet/parquet-mr/pull/270): Make ParquetInputSplit extend FileSplit

### Version 1.3.2 ###
* ISSUE [271](https://github.com/Parquet/parquet-mr/pull/271): fix bug: last enum index throws DecodingSchemaMismatchException
* ISSUE [268](https://github.com/Parquet/parquet-mr/pull/268): fixes #265: add semver validation checks to non-bundle builds
* ISSUE [269](https://github.com/Parquet/parquet-mr/pull/269): Bumps parquet-jackson parent version
* ISSUE [260](https://github.com/Parquet/parquet-mr/pull/260): Shade jackson only once for all parquet modules

### Version 1.3.1 ###
* ISSUE [267](https://github.com/Parquet/parquet-mr/pull/267): handler only handle ignored field, exception during will be thrown as Sk...
* ISSUE [266](https://github.com/Parquet/parquet-mr/pull/266): upgrade parquet-mr to elephant-bird 4.4

### Version 1.3.0 ###
* ISSUE [258](https://github.com/Parquet/parquet-mr/pull/258): Optimize scan
* ISSUE [259](https://github.com/Parquet/parquet-mr/pull/259): add delta length byte arrays and delta byte arrays encodings
* ISSUE [249](https://github.com/Parquet/parquet-mr/pull/249): make summary files read in parallel; improve memory footprint of metadata; avoid unnecessary seek
* ISSUE [257](https://github.com/Parquet/parquet-mr/pull/257): Create parquet-hadoop-bundle which will eventually replace parquet-hive-bundle
* ISSUE [253](https://github.com/Parquet/parquet-mr/pull/253): Delta Binary Packing for Int
* ISSUE [254](https://github.com/Parquet/parquet-mr/pull/254): Add writer version flag to parquet and make initial changes for supported parquet 2.0 encodings
* ISSUE [256](https://github.com/Parquet/parquet-mr/pull/256): Resolves issue #251 by doing additional checks if Hive returns "Unknown" as a version
* ISSUE [252](https://github.com/Parquet/parquet-mr/pull/252): refactor error handler for BufferedProtocolReadToWrite to be non-static

### Version 1.2.11 ###
* ISSUE [250](https://github.com/Parquet/parquet-mr/pull/250): pretty_print_json_for_compatibility_checker
* ISSUE [243](https://github.com/Parquet/parquet-mr/pull/243): add parquet cascading integration documentation
* ISSUE [248](https://github.com/Parquet/parquet-mr/pull/248): More Hadoop 2 compatibility fixes

### Version 1.2.10 ###
* ISSUE [247](https://github.com/Parquet/parquet-mr/pull/247): fix bug: when field index is greater than zero
* ISSUE [244](https://github.com/Parquet/parquet-mr/pull/244): Feature/error handler
* ISSUE [187](https://github.com/Parquet/parquet-mr/pull/187): Plumb OriginalType
* ISSUE [245](https://github.com/Parquet/parquet-mr/pull/245): integrate parquet format 2.0

### Version 1.2.9 ###
* ISSUE [242](https://github.com/Parquet/parquet-mr/pull/242): upgrade elephant-bird version to 4.3
* ISSUE [240](https://github.com/Parquet/parquet-mr/pull/240): fix loader cache
* ISSUE [233](https://github.com/Parquet/parquet-mr/pull/233): use latest stable release of cascading: 2.5.1
* ISSUE [241](https://github.com/Parquet/parquet-mr/pull/241): Update reference to 0.10 in Hive012Binding javadoc
* ISSUE [239](https://github.com/Parquet/parquet-mr/pull/239): Fix hive map and array inspectors with null containers
* ISSUE [234](https://github.com/Parquet/parquet-mr/pull/234): optimize chunk scan; fix compressed size
* ISSUE [237](https://github.com/Parquet/parquet-mr/pull/237): Handle codec not found
* ISSUE [238](https://github.com/Parquet/parquet-mr/pull/238): fix pom version caused by bad merge
* ISSUE [235](https://github.com/Parquet/parquet-mr/pull/235): Not write pig meta data only when pig is not avaliable
* ISSUE [227](https://github.com/Parquet/parquet-mr/pull/227): Breaks parquet-hive up into several submodules, creating infrastructure ...
* ISSUE [229](https://github.com/Parquet/parquet-mr/pull/229): add changelog tool
* ISSUE [236](https://github.com/Parquet/parquet-mr/pull/236): Make cascading a provided dependency

### Version 1.2.8 ###
* ISSUE 228: enable globing files for parquetTupleScheme, refactor unit tests and rem...
* ISSUE 224: Changing read and write methods in ParquetInputSplit so that they can de...

### Version 1.2.8 ###
* ISSUE 228: enable globing files for parquetTupleScheme, refactor unit tests and rem...
* ISSUE 224: Changing read and write methods in ParquetInputSplit so that they can de...

### Version 1.2.7 ###
* ISSUE 223: refactor encoded values changes and test that resetDictionary works
* ISSUE 222: fix bug: set raw data size to 0 after reset

### Version 1.2.6 ###
* ISSUE 221: make pig, hadoop and log4j jars provided
* ISSUE 220: parquet-hive should ship and uber jar
* ISSUE 213: group parquet-format version in one property
* ISSUE 215: Fix Binary.equals().
* ISSUE 210: ParquetWriter ignores enable dictionary and validating flags.
* ISSUE 202: Fix requested schema when recreating splits in hive
* ISSUE 208: Improve dic fall back
* ISSUE 207: Fix offset
* ISSUE 206: Create a "Powered by" page

### Version 1.2.5 ###
* ISSUE 204: ParquetLoader.inputFormatCache as WeakHashMap
* ISSUE 203: add null check for EnumWriteProtocol
* ISSUE 205: use cascading 2.2.0
* ISSUE 199: simplify TupleWriteSupport constructor
* ISSUE 164: Dictionary changes
* ISSUE 196: Fixes to the Hive SerDe
* ISSUE 197: RLE decoder reading past the end of the stream
* ISSUE 188: Added ability to define arbitrary predicate functions
* ISSUE 194: refactor serde to remove some unecessary boxing and include dictionary awareness
* ISSUE 190: NPE in DictionaryValuesWriter.

### Version 1.2.4 ###
* ISSUE 191: Add compatibility checker for ThriftStruct to check for backward compatibility of two thrift structs

### Version 1.2.3 ###
* ISSUE 186: add parquet-pig-bundle
* ISSUE 184: Update ParquetReader to take Configuration as a constructor argument.
* ISSUE 183: Disable the time read counter check in DeprecatedInputFormatTest.
* ISSUE 182: Fix a maven warning about a missing version number.
* ISSUE 181: FIXED_LEN_BYTE_ARRAY support
* ISSUE 180: Support writing Avro records with maps with Utf8 keys
* ISSUE 179: Added Or/Not logical filters for column predicates
* ISSUE 172: Add sink support for parquet.cascading.ParquetTBaseScheme
* ISSUE 169: Support avro records with empty maps and arrays
* ISSUE 162: Avro schema with empty arrays and maps

### Version 1.2.2 ###
* ISSUE 175: fix problem with projection pushdown in parquetloader
* ISSUE 174: improve readability by renaming variables
* ISSUE 173: make numbers in log messages easy to read in InternalParquetRecordWriter
* ISSUE 171: add unit test for parquet-scrooge
* ISSUE 165: distinguish recoverable exception in BufferedProtocolReadToWrite
* ISSUE 166: support projection when required fields in thrift class are not projected

### Version 1.2.1 ###
* ISSUE 167: fix oom error dues to bad estimation

### Version 1.2.0 ###
* ISSUE 154: improve thrift error message
* ISSUE 161: support schema evolution
* ISSUE 160: Resource leak in parquet.hadoop.ParquetFileReader.readFooter(Configurati...
* ISSUE 163: remove debugging code from hot path
* ISSUE 155: Manual pushdown for thrift read support
* ISSUE 159: Counter for mapred
* ISSUE 156: Fix site
* ISSUE 153: Fix projection required field
    
### Version 1.1.1 ###
* ISSUE 150: add thrift validation on read

### Version 1.1.0 ###
* ISSUE 149: changing default block size to 128mb  
* ISSUE 146: Fix and add unit tests for Hive nested types  
* ISSUE 145: add getStatistics method to parquetloader  
* ISSUE 144: Map key fields should allow other types than strings  
* ISSUE 143: Fix empty encoding col metadata  
* ISSUE 142: Fix total size row group  
* ISSUE 141: add parquet counters for benchmark  
* ISSUE 140: Implemented partial schema for GroupReadSupport  
* ISSUE 138: fix bug of wrong column metadata size  
* ISSUE 137: ParquetMetadataConverter bug  
* ISSUE 133: Update plugin versions for maven aether migration - fixes #125  
* ISSUE 130: Schema validation should not validate the root element's name  
* ISSUE 127: Adding dictionary encoding for non string types.. #99  
* ISSUE 125: Unable to build  
* ISSUE 124: Fix Short and Byte types in Hive SerDe.  
* ISSUE 123: Fix Snappy compressor in parquet-hadoop.  
* ISSUE 120: Fix RLE bug with partial literal groups at end of stream.  
* ISSUE 118: Refactor column reader  
* ISSUE 115: Map key fields should allow other types than strings  
* ISSUE 103: Map key fields should allow other types than strings  
* ISSUE 99: Dictionary encoding for non string types (float  double  int  long  boolean)  
* ISSUE 47: Add tests for parquet-scrooge and parquet-cascading 

### Version 1.0.1 ###
* ISSUE 126: Unit tests for parquet cascading  
* ISSUE 121: fix wrong RecordConverter for ParquetTBaseScheme  
* ISSUE 119: fix compatibility with thrift  remove unused dependency 

### Version 1.0.0 ###
