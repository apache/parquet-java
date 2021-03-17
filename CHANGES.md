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

### Version 1.12.0 ###

Release Notes - Parquet - Version 1.12.0

#### Sub-task

*   [PARQUET-1228](https://issues.apache.org/jira/browse/PARQUET-1228) - parquet-format-structures encryption
*   [PARQUET-1229](https://issues.apache.org/jira/browse/PARQUET-1229) - parquet-mr code changes for encryption support
*   [PARQUET-1286](https://issues.apache.org/jira/browse/PARQUET-1286) - Crypto package in parquet-mr
*   [PARQUET-1328](https://issues.apache.org/jira/browse/PARQUET-1328) - \[java\]Bloom filter read/write implementation
*   [PARQUET-1391](https://issues.apache.org/jira/browse/PARQUET-1391) - \[java\] Integrate Bloom filter logic
*   [PARQUET-1516](https://issues.apache.org/jira/browse/PARQUET-1516) - Store Bloom filters near to footer.
*   [PARQUET-1740](https://issues.apache.org/jira/browse/PARQUET-1740) - Make ParquetFileReader.getFilteredRecordCount public
*   [PARQUET-1744](https://issues.apache.org/jira/browse/PARQUET-1744) - Some filters throws ArrayIndexOutOfBoundsException
*   [PARQUET-1807](https://issues.apache.org/jira/browse/PARQUET-1807) - Encryption: Interop and Function test suite for Java version
*   [PARQUET-1884](https://issues.apache.org/jira/browse/PARQUET-1884) - Merge encryption branch into master
*   [PARQUET-1915](https://issues.apache.org/jira/browse/PARQUET-1915) - Add null command

#### Bug

*   [PARQUET-1438](https://issues.apache.org/jira/browse/PARQUET-1438) - \[C++\] corrupted files produced on 32-bit architecture (i686)
*   [PARQUET-1493](https://issues.apache.org/jira/browse/PARQUET-1493) - maven protobuf plugin not work properly
*   [PARQUET-1455](https://issues.apache.org/jira/browse/PARQUET-1455) - \[parquet-protobuf\] Handle "unknown" enum values for parquet-protobuf
*   [PARQUET-1554](https://issues.apache.org/jira/browse/PARQUET-1554) - Compilation error when upgrading Scrooge version
*   [PARQUET-1599](https://issues.apache.org/jira/browse/PARQUET-1599) - Fix to-avro to respect the overwrite option
*   [PARQUET-1684](https://issues.apache.org/jira/browse/PARQUET-1684) - \[parquet-protobuf\] default protobuf field values are stored as nulls
*   [PARQUET-1699](https://issues.apache.org/jira/browse/PARQUET-1699) - Could not resolve org.apache.yetus:audience-annotations:0.11.0
*   [PARQUET-1741](https://issues.apache.org/jira/browse/PARQUET-1741) - APIs backward compatibility issues cause master branch build failure
*   [PARQUET-1765](https://issues.apache.org/jira/browse/PARQUET-1765) - Invalid filteredRowCount in InternalParquetRecordReader
*   [PARQUET-1794](https://issues.apache.org/jira/browse/PARQUET-1794) - Random data generation may cause flaky tests
*   [PARQUET-1803](https://issues.apache.org/jira/browse/PARQUET-1803) - Could not find FilleInputSplit in ParquetInputSplit
*   [PARQUET-1808](https://issues.apache.org/jira/browse/PARQUET-1808) - SimpleGroup.toString() uses String += and so has poor performance
*   [PARQUET-1818](https://issues.apache.org/jira/browse/PARQUET-1818) - Fix collision of encryption and bloom filters in format-structure Util
*   [PARQUET-1850](https://issues.apache.org/jira/browse/PARQUET-1850) - toParquetMetadata method in ParquetMetadataConverter does not set dictionary page offset bit
*   [PARQUET-1851](https://issues.apache.org/jira/browse/PARQUET-1851) - ParquetMetadataConveter throws NPE in an Iceberg unit test
*   [PARQUET-1868](https://issues.apache.org/jira/browse/PARQUET-1868) - Parquet reader options toggle for bloom filter toggles dictionary filtering
*   [PARQUET-1879](https://issues.apache.org/jira/browse/PARQUET-1879) - Apache Arrow can not read a Parquet File written with Parqet-Avro 1.11.0 with a Map field
*   [PARQUET-1893](https://issues.apache.org/jira/browse/PARQUET-1893) - H2SeekableInputStream readFully() doesn't respect start and len
*   [PARQUET-1894](https://issues.apache.org/jira/browse/PARQUET-1894) - Please fix the related Shaded Jackson Databind CVEs
*   [PARQUET-1896](https://issues.apache.org/jira/browse/PARQUET-1896) - \[Maven\] parquet-tools build is broken
*   [PARQUET-1910](https://issues.apache.org/jira/browse/PARQUET-1910) - Parquet-cli is broken after TransCompressionCommand was added
*   [PARQUET-1917](https://issues.apache.org/jira/browse/PARQUET-1917) - \[parquet-proto\] default values are stored in oneOf fields that aren't set
*   [PARQUET-1920](https://issues.apache.org/jira/browse/PARQUET-1920) - Fix issue with reading parquet files with too large column chunks
*   [PARQUET-1923](https://issues.apache.org/jira/browse/PARQUET-1923) - parquet-tools 1.11.0: TestSimpleRecordConverter fails with ExceptionInInitializerError on openjdk 15
*   [PARQUET-1928](https://issues.apache.org/jira/browse/PARQUET-1928) - Interpret Parquet INT96 type as FIXED\[12\] AVRO Schema
*   [PARQUET-1944](https://issues.apache.org/jira/browse/PARQUET-1944) - Unable to download transitive dependency hadoop-lzo
*   [PARQUET-1947](https://issues.apache.org/jira/browse/PARQUET-1947) - DeprecatedParquetInputFormat in CombineFileInputFormat would produce wrong data
*   [PARQUET-1949](https://issues.apache.org/jira/browse/PARQUET-1949) - Mark Parquet-1872 with not support bloom filter yet
*   [PARQUET-1954](https://issues.apache.org/jira/browse/PARQUET-1954) - TCP connection leak in parquet dump
*   [PARQUET-1963](https://issues.apache.org/jira/browse/PARQUET-1963) - DeprecatedParquetInputFormat in CombineFileInputFormat throw NPE when the first sub-split is empty
*   [PARQUET-1966](https://issues.apache.org/jira/browse/PARQUET-1966) - Fix build with JDK11 for JDK8
*   [PARQUET-1970](https://issues.apache.org/jira/browse/PARQUET-1970) - Make minor releases source compatible
*   [PARQUET-1971](https://issues.apache.org/jira/browse/PARQUET-1971) - Flaky test in github action
*   [PARQUET-1975](https://issues.apache.org/jira/browse/PARQUET-1975) - Test failure on ARM64 CPU architecture
*   [PARQUET-1977](https://issues.apache.org/jira/browse/PARQUET-1977) - Invalid data\_page\_offset
*   [PARQUET-1979](https://issues.apache.org/jira/browse/PARQUET-1979) - Optional bloom\_filter\_offset is filled if no bloom filter is present
*   [PARQUET-1984](https://issues.apache.org/jira/browse/PARQUET-1984) - Some tests fail on windows
*   [PARQUET-1992](https://issues.apache.org/jira/browse/PARQUET-1992) - Cannot build from tarball because of git submodules
*   [PARQUET-1999](https://issues.apache.org/jira/browse/PARQUET-1999) - NPE might occur if OutputFile is implemented by the client

#### New Feature

*   [PARQUET-41](https://issues.apache.org/jira/browse/PARQUET-41) - Add bloom filters to parquet statistics
*   [PARQUET-1373](https://issues.apache.org/jira/browse/PARQUET-1373) - Encryption key management tools
*   [PARQUET-1396](https://issues.apache.org/jira/browse/PARQUET-1396) - Example of using EncryptionPropertiesFactory and DecryptionPropertiesFactory
*   [PARQUET-1622](https://issues.apache.org/jira/browse/PARQUET-1622) - Add BYTE\_STREAM\_SPLIT encoding
*   [PARQUET-1784](https://issues.apache.org/jira/browse/PARQUET-1784) - Column-wise configuration
*   [PARQUET-1817](https://issues.apache.org/jira/browse/PARQUET-1817) - Crypto Properties Factory
*   [PARQUET-1854](https://issues.apache.org/jira/browse/PARQUET-1854) - Properties-Driven Interface to Parquet Encryption

#### Improvement

*   [PARQUET-313](https://issues.apache.org/jira/browse/PARQUET-313) - Implement 3 level list writing rule for Parquet-Thrift
*   [PARQUET-1528](https://issues.apache.org/jira/browse/PARQUET-1528) - Add JSON support to \`parquet-tools head\`
*   [PARQUET-1593](https://issues.apache.org/jira/browse/PARQUET-1593) - Replace the example usage in parquet-cli's help message with an actually existent subcommand
*   [PARQUET-1660](https://issues.apache.org/jira/browse/PARQUET-1660) - \[java\] Align Bloom filter implementation with format
*   [PARQUET-1666](https://issues.apache.org/jira/browse/PARQUET-1666) - Remove Unused Modules
*   [PARQUET-1696](https://issues.apache.org/jira/browse/PARQUET-1696) - Remove unused hadoop-1 profile
*   [PARQUET-1710](https://issues.apache.org/jira/browse/PARQUET-1710) - Use Objects.requireNonNull
*   [PARQUET-1723](https://issues.apache.org/jira/browse/PARQUET-1723) - Read From Maps Without Using Contains
*   [PARQUET-1724](https://issues.apache.org/jira/browse/PARQUET-1724) - Use ConcurrentHashMap for Cache in DictionaryPageReader
*   [PARQUET-1725](https://issues.apache.org/jira/browse/PARQUET-1725) - Replace Usage of Strings.join with JDK Functionality in ColumnPath Class
*   [PARQUET-1726](https://issues.apache.org/jira/browse/PARQUET-1726) - Use Java 8 Multi Exception Handling
*   [PARQUET-1727](https://issues.apache.org/jira/browse/PARQUET-1727) - Do Not Swallow InterruptedException in ParquetLoader
*   [PARQUET-1728](https://issues.apache.org/jira/browse/PARQUET-1728) - Simplify NullPointerException Handling in AvroWriteSupport
*   [PARQUET-1729](https://issues.apache.org/jira/browse/PARQUET-1729) - Avoid AutoBoxing in EncodingStats
*   [PARQUET-1730](https://issues.apache.org/jira/browse/PARQUET-1730) - Use switch Statement in AvroIndexedRecordConverter for Enums
*   [PARQUET-1731](https://issues.apache.org/jira/browse/PARQUET-1731) - Use JDK 8 Facilities to Simplify FilteringRecordMaterializer
*   [PARQUET-1732](https://issues.apache.org/jira/browse/PARQUET-1732) - Call toArray With Empty Array
*   [PARQUET-1735](https://issues.apache.org/jira/browse/PARQUET-1735) - Clean Up parquet-columns Module
*   [PARQUET-1736](https://issues.apache.org/jira/browse/PARQUET-1736) - Use StringBuilder instead of StringBuffer
*   [PARQUET-1737](https://issues.apache.org/jira/browse/PARQUET-1737) - Replace Test Class RandomStr with Apache Commons Lang
*   [PARQUET-1738](https://issues.apache.org/jira/browse/PARQUET-1738) - Remove unused imports in parquet-column
*   [PARQUET-1743](https://issues.apache.org/jira/browse/PARQUET-1743) - Add equals to BlockSplitBloomFilter
*   [PARQUET-1749](https://issues.apache.org/jira/browse/PARQUET-1749) - Use Java 8 Streams for Empty PrimitiveIterator
*   [PARQUET-1750](https://issues.apache.org/jira/browse/PARQUET-1750) - Reduce Memory Usage of RowRanges Class
*   [PARQUET-1751](https://issues.apache.org/jira/browse/PARQUET-1751) - Fix Protobuf Build Warning
*   [PARQUET-1756](https://issues.apache.org/jira/browse/PARQUET-1756) - Remove Dependency on Maven Plugin semantic-versioning
*   [PARQUET-1759](https://issues.apache.org/jira/browse/PARQUET-1759) - InternalParquetRecordReader Use Singleton Set
*   [PARQUET-1763](https://issues.apache.org/jira/browse/PARQUET-1763) - Add SLF4J to TestCircularReferences
*   [PARQUET-1764](https://issues.apache.org/jira/browse/PARQUET-1764) - The ParquetProperties constructor parameter list is so long
*   [PARQUET-1775](https://issues.apache.org/jira/browse/PARQUET-1775) - Deprecate AvroParquetWriter Builder Hadoop Path
*   [PARQUET-1778](https://issues.apache.org/jira/browse/PARQUET-1778) - Do Not Consider Class for Avro Generic Record Reader
*   [PARQUET-1782](https://issues.apache.org/jira/browse/PARQUET-1782) - Use Switch Statement in AvroRecordConverter
*   [PARQUET-1790](https://issues.apache.org/jira/browse/PARQUET-1790) - ParquetFileWriter missing Api for DataPageV2
*   [PARQUET-1791](https://issues.apache.org/jira/browse/PARQUET-1791) - Add 'prune' command to parquet-tools
*   [PARQUET-1801](https://issues.apache.org/jira/browse/PARQUET-1801) - Add column index support for 'prune' command in Parquet-tools/cli
*   [PARQUET-1802](https://issues.apache.org/jira/browse/PARQUET-1802) - CompressionCodec class not found if the codec class is not in the same defining classloader as the CodecFactory class
*   [PARQUET-1805](https://issues.apache.org/jira/browse/PARQUET-1805) - Refactor the configuration for bloom filters
*   [PARQUET-1821](https://issues.apache.org/jira/browse/PARQUET-1821) - Add 'column-size' command to parquet-cli and parquet-tools
*   [PARQUET-1826](https://issues.apache.org/jira/browse/PARQUET-1826) - Document hadoop configuration options
*   [PARQUET-1827](https://issues.apache.org/jira/browse/PARQUET-1827) - UUID type currently not supported by parquet-mr
*   [PARQUET-1853](https://issues.apache.org/jira/browse/PARQUET-1853) - Minimize the parquet-avro fastutil shaded jar
*   [PARQUET-1863](https://issues.apache.org/jira/browse/PARQUET-1863) - Remove use of add-test-source mojo in parquet-protobuf
*   [PARQUET-1866](https://issues.apache.org/jira/browse/PARQUET-1866) - Replace Hadoop ZSTD with JNI-ZSTD
*   [PARQUET-1890](https://issues.apache.org/jira/browse/PARQUET-1890) - Upgrade to Avro 1.10.0
*   [PARQUET-1891](https://issues.apache.org/jira/browse/PARQUET-1891) - Encryption-related light fixes
*   [PARQUET-1914](https://issues.apache.org/jira/browse/PARQUET-1914) - Allow ProtoParquetReader To Support InputFile
*   [PARQUET-1924](https://issues.apache.org/jira/browse/PARQUET-1924) - Do not Instantiate a New LongHashFunction
*   [PARQUET-1926](https://issues.apache.org/jira/browse/PARQUET-1926) - Add LogicalType support to ThriftType.I64Type
*   [PARQUET-1929](https://issues.apache.org/jira/browse/PARQUET-1929) - Bump Snappy to 1.1.8
*   [PARQUET-1930](https://issues.apache.org/jira/browse/PARQUET-1930) - Bump Apache Thrift to 0.13.0
*   [PARQUET-1931](https://issues.apache.org/jira/browse/PARQUET-1931) - Bump Junit 4.13.1
*   [PARQUET-1932](https://issues.apache.org/jira/browse/PARQUET-1932) - Bump Fastutil to 8.4.2
*   [PARQUET-1938](https://issues.apache.org/jira/browse/PARQUET-1938) - Option to get KMS details from key material (in key rotation)
*   [PARQUET-1939](https://issues.apache.org/jira/browse/PARQUET-1939) - Fix RemoteKmsClient API ambiguity
*   [PARQUET-1940](https://issues.apache.org/jira/browse/PARQUET-1940) - Make KeyEncryptionKey length configurable
*   [PARQUET-1941](https://issues.apache.org/jira/browse/PARQUET-1941) - Bump Commons CLI from 1.3.1 to 1.4
*   [PARQUET-1951](https://issues.apache.org/jira/browse/PARQUET-1951) - Allow different strategies to combine key values when merging parquet files
*   [PARQUET-1952](https://issues.apache.org/jira/browse/PARQUET-1952) - Upgrade Avro to 1.10.1
*   [PARQUET-1961](https://issues.apache.org/jira/browse/PARQUET-1961) - Bump Jackson to 2.11.4
*   [PARQUET-1964](https://issues.apache.org/jira/browse/PARQUET-1964) - Properly handle missing/null filter
*   [PARQUET-1967](https://issues.apache.org/jira/browse/PARQUET-1967) - Upgrade Zstd-jni to 1.4.8-3
*   [PARQUET-1969](https://issues.apache.org/jira/browse/PARQUET-1969) - Test by GithubAction
*   [PARQUET-1973](https://issues.apache.org/jira/browse/PARQUET-1973) - Support ZSTD JNI BufferPool
*   [PARQUET-1988](https://issues.apache.org/jira/browse/PARQUET-1988) - Upgrade to ZSTD 1.4.8-6
*   [PARQUET-1994](https://issues.apache.org/jira/browse/PARQUET-1994) - Upgrade ZSTD JNI to 1.4.9-1

#### Test

*   [PARQUET-1832](https://issues.apache.org/jira/browse/PARQUET-1832) - Travis fails with too long output
*   [PARQUET-1980](https://issues.apache.org/jira/browse/PARQUET-1980) - Build and test Apache Parquet on ARM64 CPU architecture

#### Wish

*   [PARQUET-1717](https://issues.apache.org/jira/browse/PARQUET-1717) - parquet-thrift converts Thrift i16 to parquet INT32 instead of INT\_16

#### Task

*   [PARQUET-1676](https://issues.apache.org/jira/browse/PARQUET-1676) - Remove hive modules
*   [PARQUET-1703](https://issues.apache.org/jira/browse/PARQUET-1703) - Update API compatibility check
*   [PARQUET-1796](https://issues.apache.org/jira/browse/PARQUET-1796) - Bump Apache Avro to 1.9.2
*   [PARQUET-1842](https://issues.apache.org/jira/browse/PARQUET-1842) - Update Jackson Databind version to address CVE
*   [PARQUET-1844](https://issues.apache.org/jira/browse/PARQUET-1844) - Removed Hadoop transitive dependency on commons-lang
*   [PARQUET-1895](https://issues.apache.org/jira/browse/PARQUET-1895) - Update jackson-databind
*   [PARQUET-1898](https://issues.apache.org/jira/browse/PARQUET-1898) - Release parquet-mr 1.12.0

### Version 1.11.0 ###

Release Notes - Parquet - Version 1.11.0

#### Bug

*   [PARQUET-138](https://issues.apache.org/jira/browse/PARQUET-138) - Parquet should allow a merge between required and optional schemas
*   [PARQUET-952](https://issues.apache.org/jira/browse/PARQUET-952) - Avro union with single type fails with 'is not a group'
*   [PARQUET-1128](https://issues.apache.org/jira/browse/PARQUET-1128) - \[Java\] Upgrade the Apache Arrow version to 0.8.0 for SchemaConverter
*   [PARQUET-1281](https://issues.apache.org/jira/browse/PARQUET-1281) - Jackson dependency
*   [PARQUET-1285](https://issues.apache.org/jira/browse/PARQUET-1285) - \[Java\] SchemaConverter should not convert from TimeUnit.SECOND AND TimeUnit.NANOSECOND of Arrow
*   [PARQUET-1293](https://issues.apache.org/jira/browse/PARQUET-1293) - Build failure when using Java 8 lambda expressions
*   [PARQUET-1296](https://issues.apache.org/jira/browse/PARQUET-1296) - Travis kills build after 10 minutes, because "no output was received"
*   [PARQUET-1297](https://issues.apache.org/jira/browse/PARQUET-1297) - \[Java\] SchemaConverter should not convert from Timestamp(TimeUnit.SECOND) and Timestamp(TimeUnit.NANOSECOND) of Arrow
*   [PARQUET-1303](https://issues.apache.org/jira/browse/PARQUET-1303) - Avro reflect @Stringable field write error if field not instanceof CharSequence
*   [PARQUET-1304](https://issues.apache.org/jira/browse/PARQUET-1304) - Release 1.10 contains breaking changes for Hive
*   [PARQUET-1305](https://issues.apache.org/jira/browse/PARQUET-1305) - Backward incompatible change introduced in 1.8
*   [PARQUET-1309](https://issues.apache.org/jira/browse/PARQUET-1309) - Parquet Java uses incorrect stats and dictionary filter properties
*   [PARQUET-1311](https://issues.apache.org/jira/browse/PARQUET-1311) - Update README.md
*   [PARQUET-1317](https://issues.apache.org/jira/browse/PARQUET-1317) - ParquetMetadataConverter throw NPE
*   [PARQUET-1341](https://issues.apache.org/jira/browse/PARQUET-1341) - Null count is suppressed when columns have no min or max and use unsigned sort order
*   [PARQUET-1344](https://issues.apache.org/jira/browse/PARQUET-1344) - Type builders don't honor new logical types
*   [PARQUET-1368](https://issues.apache.org/jira/browse/PARQUET-1368) - ParquetFileReader should close its input stream for the failure in constructor
*   [PARQUET-1371](https://issues.apache.org/jira/browse/PARQUET-1371) - Time/Timestamp UTC normalization parameter doesn't work
*   [PARQUET-1407](https://issues.apache.org/jira/browse/PARQUET-1407) - Data loss on duplicate values with AvroParquetWriter/Reader
*   [PARQUET-1417](https://issues.apache.org/jira/browse/PARQUET-1417) - BINARY\_AS\_SIGNED\_INTEGER\_COMPARATOR fails with IOBE for the same arrays with the different length
*   [PARQUET-1421](https://issues.apache.org/jira/browse/PARQUET-1421) - InternalParquetRecordWriter logs debug messages at the INFO level
*   [PARQUET-1440](https://issues.apache.org/jira/browse/PARQUET-1440) - Parquet-tools: Decimal values stored in an int32 or int64 in the parquet file aren't displayed with their proper scale
*   [PARQUET-1441](https://issues.apache.org/jira/browse/PARQUET-1441) - SchemaParseException: Can't redefine: list in AvroIndexedRecordConverter
*   [PARQUET-1456](https://issues.apache.org/jira/browse/PARQUET-1456) - Use page index, ParquetFileReader throw ArrayIndexOutOfBoundsException
*   [PARQUET-1460](https://issues.apache.org/jira/browse/PARQUET-1460) - Fix javadoc errors and include javadoc checking in Travis checks
*   [PARQUET-1461](https://issues.apache.org/jira/browse/PARQUET-1461) - Third party code does not compile after parquet-mr minor version update
*   [PARQUET-1470](https://issues.apache.org/jira/browse/PARQUET-1470) - Inputstream leakage in ParquetFileWriter.appendFile
*   [PARQUET-1472](https://issues.apache.org/jira/browse/PARQUET-1472) - Dictionary filter fails on FIXED\_LEN\_BYTE\_ARRAY
*   [PARQUET-1475](https://issues.apache.org/jira/browse/PARQUET-1475) - DirectCodecFactory's ParquetCompressionCodecException drops a passed in cause in one constructor
*   [PARQUET-1478](https://issues.apache.org/jira/browse/PARQUET-1478) - Can't read spec compliant, 3-level lists via parquet-proto
*   [PARQUET-1480](https://issues.apache.org/jira/browse/PARQUET-1480) - INT96 to avro not yet implemented error should mention deprecation
*   [PARQUET-1485](https://issues.apache.org/jira/browse/PARQUET-1485) - Snappy Decompressor/Compressor may cause direct memory leak
*   [PARQUET-1488](https://issues.apache.org/jira/browse/PARQUET-1488) - UserDefinedPredicate throw NPE
*   [PARQUET-1496](https://issues.apache.org/jira/browse/PARQUET-1496) - \[Java\] Update Scala for JDK 11 compatibility
*   [PARQUET-1497](https://issues.apache.org/jira/browse/PARQUET-1497) - \[Java\] javax annotations dependency missing for Java 11
*   [PARQUET-1498](https://issues.apache.org/jira/browse/PARQUET-1498) - \[Java\] Add instructions to install thrift via homebrew
*   [PARQUET-1510](https://issues.apache.org/jira/browse/PARQUET-1510) - Dictionary filter skips null values when evaluating not-equals.
*   [PARQUET-1514](https://issues.apache.org/jira/browse/PARQUET-1514) - ParquetFileWriter Records Compressed Bytes instead of Uncompressed Bytes
*   [PARQUET-1527](https://issues.apache.org/jira/browse/PARQUET-1527) - \[parquet-tools\] cat command throw java.lang.ClassCastException
*   [PARQUET-1529](https://issues.apache.org/jira/browse/PARQUET-1529) - Shade fastutil in all modules where used
*   [PARQUET-1531](https://issues.apache.org/jira/browse/PARQUET-1531) - Page row count limit causes empty pages to be written from MessageColumnIO
*   [PARQUET-1533](https://issues.apache.org/jira/browse/PARQUET-1533) - TestSnappy() throws OOM exception with Parquet-1485 change
*   [PARQUET-1534](https://issues.apache.org/jira/browse/PARQUET-1534) - \[parquet-cli\] Argument error: Illegal character in opaque part at index 2 on Windows
*   [PARQUET-1544](https://issues.apache.org/jira/browse/PARQUET-1544) - Possible over-shading of modules
*   [PARQUET-1550](https://issues.apache.org/jira/browse/PARQUET-1550) - CleanUtil does not work in Java 11
*   [PARQUET-1555](https://issues.apache.org/jira/browse/PARQUET-1555) - Bump snappy-java to 1.1.7.3
*   [PARQUET-1596](https://issues.apache.org/jira/browse/PARQUET-1596) - PARQUET-1375 broke parquet-cli's to-avro command
*   [PARQUET-1600](https://issues.apache.org/jira/browse/PARQUET-1600) - Fix shebang in parquet-benchmarks/run.sh
*   [PARQUET-1615](https://issues.apache.org/jira/browse/PARQUET-1615) - getRecordWriter shouldn't hardcode CREAT mode when new ParquetFileWriter
*   [PARQUET-1637](https://issues.apache.org/jira/browse/PARQUET-1637) - Builds are failing because default jdk changed to openjdk11 on Travis
*   [PARQUET-1644](https://issues.apache.org/jira/browse/PARQUET-1644) - Clean up some benchmark code and docs.
*   [PARQUET-1691](https://issues.apache.org/jira/browse/PARQUET-1691) - Build fails due to missing hadoop-lzo

#### New Feature

*   [PARQUET-1201](https://issues.apache.org/jira/browse/PARQUET-1201) - Column indexes
*   [PARQUET-1253](https://issues.apache.org/jira/browse/PARQUET-1253) - Support for new logical type representation
*   [PARQUET-1388](https://issues.apache.org/jira/browse/PARQUET-1388) - Nanosecond precision time and timestamp - parquet-mr

#### Improvement

*   [PARQUET-1135](https://issues.apache.org/jira/browse/PARQUET-1135) - upgrade thrift and protobuf dependencies
*   [PARQUET-1280](https://issues.apache.org/jira/browse/PARQUET-1280) - \[parquet-protobuf\] Use maven protoc plugin
*   [PARQUET-1321](https://issues.apache.org/jira/browse/PARQUET-1321) - LogicalTypeAnnotation.LogicalTypeAnnotationVisitor#visit methods should have a return value
*   [PARQUET-1335](https://issues.apache.org/jira/browse/PARQUET-1335) - Logical type names in parquet-mr are not consistent with parquet-format
*   [PARQUET-1336](https://issues.apache.org/jira/browse/PARQUET-1336) - PrimitiveComparator should implements Serializable
*   [PARQUET-1365](https://issues.apache.org/jira/browse/PARQUET-1365) - Don't write page level statistics
*   [PARQUET-1375](https://issues.apache.org/jira/browse/PARQUET-1375) - Upgrade to supported version of Jackson
*   [PARQUET-1383](https://issues.apache.org/jira/browse/PARQUET-1383) - Parquet tools should indicate UTC parameter for time/timestamp types
*   [PARQUET-1390](https://issues.apache.org/jira/browse/PARQUET-1390) - \[Java\] Upgrade to Arrow 0.10.0
*   [PARQUET-1399](https://issues.apache.org/jira/browse/PARQUET-1399) - Move parquet-mr related code from parquet-format
*   [PARQUET-1410](https://issues.apache.org/jira/browse/PARQUET-1410) - Refactor modules to use the new logical type API
*   [PARQUET-1414](https://issues.apache.org/jira/browse/PARQUET-1414) - Limit page size based on maximum row count
*   [PARQUET-1418](https://issues.apache.org/jira/browse/PARQUET-1418) - Run integration tests in Travis
*   [PARQUET-1435](https://issues.apache.org/jira/browse/PARQUET-1435) - Benchmark filtering column-indexes
*   [PARQUET-1444](https://issues.apache.org/jira/browse/PARQUET-1444) - Prefer ArrayList over LinkedList
*   [PARQUET-1445](https://issues.apache.org/jira/browse/PARQUET-1445) - Remove Files.java
*   [PARQUET-1462](https://issues.apache.org/jira/browse/PARQUET-1462) - Allow specifying new development version in prepare-release.sh
*   [PARQUET-1466](https://issues.apache.org/jira/browse/PARQUET-1466) - Upgrade to the latest guava 27.0-jre
*   [PARQUET-1474](https://issues.apache.org/jira/browse/PARQUET-1474) - Less verbose and lower level logging for missing column/offset indexes
*   [PARQUET-1476](https://issues.apache.org/jira/browse/PARQUET-1476) - Don't emit a warning message for files without new logical type
*   [PARQUET-1487](https://issues.apache.org/jira/browse/PARQUET-1487) - Do not write original type for timezone-agnostic timestamps
*   [PARQUET-1489](https://issues.apache.org/jira/browse/PARQUET-1489) - Insufficient documentation for UserDefinedPredicate.keep(T)
*   [PARQUET-1490](https://issues.apache.org/jira/browse/PARQUET-1490) - Add branch-specific Travis steps
*   [PARQUET-1492](https://issues.apache.org/jira/browse/PARQUET-1492) - Remove protobuf install in travis build
*   [PARQUET-1499](https://issues.apache.org/jira/browse/PARQUET-1499) - \[parquet-mr\] Add Java 11 to Travis
*   [PARQUET-1500](https://issues.apache.org/jira/browse/PARQUET-1500) - Remove the Closables
*   [PARQUET-1502](https://issues.apache.org/jira/browse/PARQUET-1502) - Convert FIXED\_LEN\_BYTE\_ARRAY to arrow type in logicalTypeAnnotation if it is not null
*   [PARQUET-1503](https://issues.apache.org/jira/browse/PARQUET-1503) - Remove Ints Utility Class
*   [PARQUET-1504](https://issues.apache.org/jira/browse/PARQUET-1504) - Add an option to convert Parquet Int96 to Arrow Timestamp
*   [PARQUET-1505](https://issues.apache.org/jira/browse/PARQUET-1505) - Use Java 7 NIO StandardCharsets
*   [PARQUET-1506](https://issues.apache.org/jira/browse/PARQUET-1506) - Migrate from maven-thrift-plugin to thrift-maven-plugin
*   [PARQUET-1507](https://issues.apache.org/jira/browse/PARQUET-1507) - Bump Apache Thrift to 0.12.0
*   [PARQUET-1509](https://issues.apache.org/jira/browse/PARQUET-1509) - Update Docs for Hive Deprecation
*   [PARQUET-1513](https://issues.apache.org/jira/browse/PARQUET-1513) - HiddenFileFilter Streamline
*   [PARQUET-1518](https://issues.apache.org/jira/browse/PARQUET-1518) - Bump Jackson2 version of parquet-cli
*   [PARQUET-1530](https://issues.apache.org/jira/browse/PARQUET-1530) - Remove Dependency on commons-codec
*   [PARQUET-1542](https://issues.apache.org/jira/browse/PARQUET-1542) - Merge multiple I/O to one time I/O when read footer
*   [PARQUET-1557](https://issues.apache.org/jira/browse/PARQUET-1557) - Replace deprecated Apache Avro methods
*   [PARQUET-1558](https://issues.apache.org/jira/browse/PARQUET-1558) - Use try-with-resource in Apache Avro tests
*   [PARQUET-1576](https://issues.apache.org/jira/browse/PARQUET-1576) - Upgrade to Avro 1.9.0
*   [PARQUET-1577](https://issues.apache.org/jira/browse/PARQUET-1577) - Remove duplicate license
*   [PARQUET-1578](https://issues.apache.org/jira/browse/PARQUET-1578) - Introduce Lambdas
*   [PARQUET-1579](https://issues.apache.org/jira/browse/PARQUET-1579) - Add Github PR template
*   [PARQUET-1580](https://issues.apache.org/jira/browse/PARQUET-1580) - Page-level CRC checksum verification for DataPageV1
*   [PARQUET-1601](https://issues.apache.org/jira/browse/PARQUET-1601) - Add zstd support to parquet-cli to-avro
*   [PARQUET-1604](https://issues.apache.org/jira/browse/PARQUET-1604) - Bump fastutil from 7.0.13 to 8.2.3
*   [PARQUET-1605](https://issues.apache.org/jira/browse/PARQUET-1605) - Bump maven-javadoc-plugin from 2.9 to 3.1.0
*   [PARQUET-1606](https://issues.apache.org/jira/browse/PARQUET-1606) - Fix invalid tests scope
*   [PARQUET-1607](https://issues.apache.org/jira/browse/PARQUET-1607) - Remove duplicate maven-enforcer-plugin
*   [PARQUET-1616](https://issues.apache.org/jira/browse/PARQUET-1616) - Enable Maven batch mode
*   [PARQUET-1650](https://issues.apache.org/jira/browse/PARQUET-1650) - Implement unit test to validate column/offset indexes
*   [PARQUET-1654](https://issues.apache.org/jira/browse/PARQUET-1654) - Remove unnecessary options when building thrift
*   [PARQUET-1661](https://issues.apache.org/jira/browse/PARQUET-1661) - Upgrade to Avro 1.9.1
*   [PARQUET-1662](https://issues.apache.org/jira/browse/PARQUET-1662) - Upgrade Jackson to version 2.9.10
*   [PARQUET-1665](https://issues.apache.org/jira/browse/PARQUET-1665) - Upgrade zstd-jni to 1.4.0-1
*   [PARQUET-1669](https://issues.apache.org/jira/browse/PARQUET-1669) - Disable compiling all libraries when building thrift
*   [PARQUET-1671](https://issues.apache.org/jira/browse/PARQUET-1671) - Upgrade Yetus to 0.11.0
*   [PARQUET-1682](https://issues.apache.org/jira/browse/PARQUET-1682) - Maintain forward compatibility for TIME/TIMESTAMP
*   [PARQUET-1683](https://issues.apache.org/jira/browse/PARQUET-1683) - Remove unnecessary string converting in readFooter method
*   [PARQUET-1685](https://issues.apache.org/jira/browse/PARQUET-1685) - Truncate the stored min and max for String statistics to reduce the footer size

#### Test

*   [PARQUET-1536](https://issues.apache.org/jira/browse/PARQUET-1536) - \[parquet-cli\] Add simple tests for each command

#### Wish

*   [PARQUET-1552](https://issues.apache.org/jira/browse/PARQUET-1552) - upgrade protoc-jar-maven-plugin to 3.8.0
*   [PARQUET-1673](https://issues.apache.org/jira/browse/PARQUET-1673) - Upgrade parquet-mr format version to 2.7.0

#### Task

*   [PARQUET-968](https://issues.apache.org/jira/browse/PARQUET-968) - Add Hive/Presto support in ProtoParquet
*   [PARQUET-1294](https://issues.apache.org/jira/browse/PARQUET-1294) - Update release scripts for the new Apache policy
*   [PARQUET-1434](https://issues.apache.org/jira/browse/PARQUET-1434) - Release parquet-mr 1.11.0
*   [PARQUET-1436](https://issues.apache.org/jira/browse/PARQUET-1436) - TimestampMicrosStringifier shows wrong microseconds for timestamps before 1970
*   [PARQUET-1452](https://issues.apache.org/jira/browse/PARQUET-1452) - Deprecate old logical types API
*   [PARQUET-1551](https://issues.apache.org/jira/browse/PARQUET-1551) - Support Java 11 - top-level JIRA
*   [PARQUET-1570](https://issues.apache.org/jira/browse/PARQUET-1570) - Publish 1.11.0 to maven central
*   [PARQUET-1585](https://issues.apache.org/jira/browse/PARQUET-1585) - Update old external links in the code base
*   [PARQUET-1645](https://issues.apache.org/jira/browse/PARQUET-1645) - Bump Apache Avro to 1.9.1
*   [PARQUET-1649](https://issues.apache.org/jira/browse/PARQUET-1649) - Bump Jackson Databind to 2.9.9.3
*   [PARQUET-1687](https://issues.apache.org/jira/browse/PARQUET-1687) - Update release process

### Version 1.10.1 ###

Release Notes - Parquet - Version 1.10.1

#### Bug

*   [PARQUET-1510](https://issues.apache.org/jira/browse/PARQUET-1510) \- Dictionary filter skips null values when evaluating not-equals.
*   [PARQUET-1309](https://issues.apache.org/jira/browse/PARQUET-1309) \- Parquet Java uses incorrect stats and dictionary filter properties

### Version 1.10.0 ###

Release Notes - Parquet - Version 1.10.0

#### Bug

*   [PARQUET-196](https://issues.apache.org/jira/browse/PARQUET-196) \- parquet-tools command to get rowcount & size
*   [PARQUET-357](https://issues.apache.org/jira/browse/PARQUET-357) \- Parquet-thrift generates wrong schema for Thrift binary fields
*   [PARQUET-765](https://issues.apache.org/jira/browse/PARQUET-765) \- Upgrade Avro to 1.8.1
*   [PARQUET-783](https://issues.apache.org/jira/browse/PARQUET-783) \- H2SeekableInputStream does not close its underlying FSDataInputStream, leading to connection leaks
*   [PARQUET-786](https://issues.apache.org/jira/browse/PARQUET-786) \- parquet-tools README incorrectly has 'java jar' instead of 'java -jar'
*   [PARQUET-791](https://issues.apache.org/jira/browse/PARQUET-791) \- Predicate pushing down on missing columns should work on UserDefinedPredicate too
*   [PARQUET-1005](https://issues.apache.org/jira/browse/PARQUET-1005) \- Fix DumpCommand parsing to allow column projection
*   [PARQUET-1028](https://issues.apache.org/jira/browse/PARQUET-1028) \- \[JAVA\] When reading old Spark-generated files with INT96, stats are reported as valid when they aren't
*   [PARQUET-1065](https://issues.apache.org/jira/browse/PARQUET-1065) \- Deprecate type-defined sort ordering for INT96 type
*   [PARQUET-1077](https://issues.apache.org/jira/browse/PARQUET-1077) \- \[MR\] Switch to long key ids in KEYs file
*   [PARQUET-1141](https://issues.apache.org/jira/browse/PARQUET-1141) \- IDs are dropped in metadata conversion
*   [PARQUET-1152](https://issues.apache.org/jira/browse/PARQUET-1152) \- Parquet-thrift doesn't compile with Thrift 0.9.3
*   [PARQUET-1153](https://issues.apache.org/jira/browse/PARQUET-1153) \- Parquet-thrift doesn't compile with Thrift 0.10.0
*   [PARQUET-1156](https://issues.apache.org/jira/browse/PARQUET-1156) \- dev/merge\_parquet\_pr.py problems
*   [PARQUET-1185](https://issues.apache.org/jira/browse/PARQUET-1185) \- TestBinary#testBinary unit test fails after PARQUET-1141
*   [PARQUET-1191](https://issues.apache.org/jira/browse/PARQUET-1191) \- Type.hashCode() takes originalType into account but Type.equals() does not
*   [PARQUET-1208](https://issues.apache.org/jira/browse/PARQUET-1208) \- Occasional endless loop in unit test
*   [PARQUET-1217](https://issues.apache.org/jira/browse/PARQUET-1217) \- Incorrect handling of missing values in Statistics
*   [PARQUET-1246](https://issues.apache.org/jira/browse/PARQUET-1246) \- Ignore float/double statistics in case of NaN
*   [PARQUET-1258](https://issues.apache.org/jira/browse/PARQUET-1258) \- Update scm developer connection to github

#### New Feature

*   [PARQUET-1025](https://issues.apache.org/jira/browse/PARQUET-1025) \- Support new min-max statistics in parquet-mr

#### Improvement

*   [PARQUET-220](https://issues.apache.org/jira/browse/PARQUET-220) \- Unnecessary warning in ParquetRecordReader.initialize
*   [PARQUET-321](https://issues.apache.org/jira/browse/PARQUET-321) \- Set the HDFS padding default to 8MB
*   [PARQUET-386](https://issues.apache.org/jira/browse/PARQUET-386) \- Printing out the statistics of metadata in parquet-tools
*   [PARQUET-423](https://issues.apache.org/jira/browse/PARQUET-423) \- Make writing Avro to Parquet less noisy
*   [PARQUET-755](https://issues.apache.org/jira/browse/PARQUET-755) \- create parquet-arrow module with schema converter
*   [PARQUET-777](https://issues.apache.org/jira/browse/PARQUET-777) \- Add new Parquet CLI tools
*   [PARQUET-787](https://issues.apache.org/jira/browse/PARQUET-787) \- Add a size limit for heap allocations when reading
*   [PARQUET-801](https://issues.apache.org/jira/browse/PARQUET-801) \- Allow UserDefinedPredicates in DictionaryFilter
*   [PARQUET-852](https://issues.apache.org/jira/browse/PARQUET-852) \- Slowly ramp up sizes of byte\[\] in ByteBasedBitPackingEncoder
*   [PARQUET-884](https://issues.apache.org/jira/browse/PARQUET-884) \- Add support for Decimal datatype to Parquet-Pig record reader
*   [PARQUET-969](https://issues.apache.org/jira/browse/PARQUET-969) \- Decimal datatype support for parquet-tools output
*   [PARQUET-990](https://issues.apache.org/jira/browse/PARQUET-990) \- More detailed error messages in footer parsing
*   [PARQUET-1024](https://issues.apache.org/jira/browse/PARQUET-1024) \- allow for case insensitive parquet-xxx prefix in PR title
*   [PARQUET-1026](https://issues.apache.org/jira/browse/PARQUET-1026) \- allow unsigned binary stats when min == max
*   [PARQUET-1115](https://issues.apache.org/jira/browse/PARQUET-1115) \- Warn users when misusing parquet-tools merge
*   [PARQUET-1135](https://issues.apache.org/jira/browse/PARQUET-1135) \- upgrade thrift and protobuf dependencies
*   [PARQUET-1142](https://issues.apache.org/jira/browse/PARQUET-1142) \- Avoid leaking Hadoop API to downstream libraries
*   [PARQUET-1149](https://issues.apache.org/jira/browse/PARQUET-1149) \- Upgrade Avro dependancy to 1.8.2
*   [PARQUET-1170](https://issues.apache.org/jira/browse/PARQUET-1170) \- Logical-type-based toString for proper representeation in tools/logs
*   [PARQUET-1183](https://issues.apache.org/jira/browse/PARQUET-1183) \- AvroParquetWriter needs OutputFile based Builder
*   [PARQUET-1197](https://issues.apache.org/jira/browse/PARQUET-1197) \- Log rat failures
*   [PARQUET-1198](https://issues.apache.org/jira/browse/PARQUET-1198) \- Bump java source and target to java8
*   [PARQUET-1215](https://issues.apache.org/jira/browse/PARQUET-1215) \- Add accessor for footer after a file is closed
*   [PARQUET-1263](https://issues.apache.org/jira/browse/PARQUET-1263) \- ParquetReader's builder should use Configuration from the InputFile

#### Task

*   [PARQUET-768](https://issues.apache.org/jira/browse/PARQUET-768) \- Add Uwe L. Korn to KEYS
*   [PARQUET-1189](https://issues.apache.org/jira/browse/PARQUET-1189) \- Release Parquet Java 1.10

### Version 1.9.0 ###

#### Bug

*   [PARQUET-182](https://issues.apache.org/jira/browse/PARQUET-182) - FilteredRecordReader skips rows it shouldn't for schema with optional columns
*   [PARQUET-212](https://issues.apache.org/jira/browse/PARQUET-212) - Implement nested type read rules in parquet-thrift
*   [PARQUET-241](https://issues.apache.org/jira/browse/PARQUET-241) - ParquetInputFormat.getFooters() should return in the same order as what listStatus() returns
*   [PARQUET-305](https://issues.apache.org/jira/browse/PARQUET-305) - Logger instantiated for package org.apache.parquet may be GC-ed
*   [PARQUET-335](https://issues.apache.org/jira/browse/PARQUET-335) - Avro object model should not require MAP_KEY_VALUE
*   [PARQUET-340](https://issues.apache.org/jira/browse/PARQUET-340) - totalMemoryPool is truncated to 32 bits
*   [PARQUET-346](https://issues.apache.org/jira/browse/PARQUET-346) - ThriftSchemaConverter throws for unknown struct or union type
*   [PARQUET-349](https://issues.apache.org/jira/browse/PARQUET-349) - VersionParser does not handle versions like "parquet-mr 1.6.0rc4"
*   [PARQUET-352](https://issues.apache.org/jira/browse/PARQUET-352) - Add tags to "created by" metadata in the file footer
*   [PARQUET-353](https://issues.apache.org/jira/browse/PARQUET-353) - Compressors not getting recycled while writing parquet files, causing memory leak
*   [PARQUET-360](https://issues.apache.org/jira/browse/PARQUET-360) - parquet-cat json dump is broken for maps
*   [PARQUET-363](https://issues.apache.org/jira/browse/PARQUET-363) - Cannot construct empty MessageType for ReadContext.requestedSchema
*   [PARQUET-367](https://issues.apache.org/jira/browse/PARQUET-367) - "parquet-cat -j" doesn't show all records
*   [PARQUET-372](https://issues.apache.org/jira/browse/PARQUET-372) - Parquet stats can have awkwardly large values
*   [PARQUET-373](https://issues.apache.org/jira/browse/PARQUET-373) - MemoryManager tests are flaky
*   [PARQUET-379](https://issues.apache.org/jira/browse/PARQUET-379) - PrimitiveType.union erases original type
*   [PARQUET-380](https://issues.apache.org/jira/browse/PARQUET-380) - Cascading and scrooge builds fail when using thrift 0.9.0
*   [PARQUET-385](https://issues.apache.org/jira/browse/PARQUET-385) - PrimitiveType.union accepts fixed_len_byte_array fields with different lengths when strict mode is on
*   [PARQUET-387](https://issues.apache.org/jira/browse/PARQUET-387) - TwoLevelListWriter does not handle null values in array
*   [PARQUET-389](https://issues.apache.org/jira/browse/PARQUET-389) - Filter predicates should work with missing columns
*   [PARQUET-395](https://issues.apache.org/jira/browse/PARQUET-395) - System.out is used as logger in org.apache.parquet.Log
*   [PARQUET-396](https://issues.apache.org/jira/browse/PARQUET-396) - The builder for AvroParquetReader loses the record type
*   [PARQUET-400](https://issues.apache.org/jira/browse/PARQUET-400) - Error reading some files after PARQUET-77 bytebuffer read path
*   [PARQUET-409](https://issues.apache.org/jira/browse/PARQUET-409) - InternalParquetRecordWriter doesn't use min/max row counts
*   [PARQUET-410](https://issues.apache.org/jira/browse/PARQUET-410) - Fix subprocess hang in merge_parquet_pr.py
*   [PARQUET-413](https://issues.apache.org/jira/browse/PARQUET-413) - Test failures for Java 8
*   [PARQUET-415](https://issues.apache.org/jira/browse/PARQUET-415) - ByteBufferBackedBinary serialization is broken
*   [PARQUET-422](https://issues.apache.org/jira/browse/PARQUET-422) - Fix a potential bug in MessageTypeParser where we ignore and overwrite the initial value of a method parameter
*   [PARQUET-425](https://issues.apache.org/jira/browse/PARQUET-425) - Fix the bug when predicate contains columns not specified in prejection, to prevent filtering out data improperly
*   [PARQUET-426](https://issues.apache.org/jira/browse/PARQUET-426) - Throw Exception when predicate contains columns not specified in prejection, to prevent filtering out data improperly
*   [PARQUET-430](https://issues.apache.org/jira/browse/PARQUET-430) - Change to use Locale parameterized version of String.toUpperCase()/toLowerCase
*   [PARQUET-431](https://issues.apache.org/jira/browse/PARQUET-431) - Make ParquetOutputFormat.memoryManager volatile
*   [PARQUET-495](https://issues.apache.org/jira/browse/PARQUET-495) - Fix mismatches in Types class comments
*   [PARQUET-509](https://issues.apache.org/jira/browse/PARQUET-509) - Incorrect number of args passed to string.format calls
*   [PARQUET-511](https://issues.apache.org/jira/browse/PARQUET-511) - Integer overflow on counting values in column
*   [PARQUET-528](https://issues.apache.org/jira/browse/PARQUET-528) - Fix flush() for RecordConsumer and implementations
*   [PARQUET-529](https://issues.apache.org/jira/browse/PARQUET-529) - Avoid evoking job.toString() in ParquetLoader
*   [PARQUET-540](https://issues.apache.org/jira/browse/PARQUET-540) - Cascading3 module doesn't build when using thrift 0.9.0
*   [PARQUET-544](https://issues.apache.org/jira/browse/PARQUET-544) - ParquetWriter.close() throws NullPointerException on second call, improper implementation of Closeable contract
*   [PARQUET-560](https://issues.apache.org/jira/browse/PARQUET-560) - Incorrect synchronization in SnappyCompressor
*   [PARQUET-569](https://issues.apache.org/jira/browse/PARQUET-569) - ParquetMetadataConverter offset filter is broken
*   [PARQUET-571](https://issues.apache.org/jira/browse/PARQUET-571) - Fix potential leak in ParquetFileReader.close()
*   [PARQUET-580](https://issues.apache.org/jira/browse/PARQUET-580) - Potentially unnecessary creation of large int[] in IntList for columns that aren't used
*   [PARQUET-581](https://issues.apache.org/jira/browse/PARQUET-581) - Min/max row count for page size check are conflated in some places
*   [PARQUET-584](https://issues.apache.org/jira/browse/PARQUET-584) - show proper command usage when there's no arguments
*   [PARQUET-612](https://issues.apache.org/jira/browse/PARQUET-612) - Add compression to FileEncodingIT tests
*   [PARQUET-623](https://issues.apache.org/jira/browse/PARQUET-623) - DeltaByteArrayReader has incorrect skip behaviour
*   [PARQUET-642](https://issues.apache.org/jira/browse/PARQUET-642) - Improve performance of ByteBuffer based read / write paths
*   [PARQUET-645](https://issues.apache.org/jira/browse/PARQUET-645) - DictionaryFilter incorrectly handles null
*   [PARQUET-651](https://issues.apache.org/jira/browse/PARQUET-651) - Parquet-avro fails to decode array of record with a single field name "element" correctly
*   [PARQUET-660](https://issues.apache.org/jira/browse/PARQUET-660) - Writing Protobuf messages with extensions results in an error or data corruption.
*   [PARQUET-663](https://issues.apache.org/jira/browse/PARQUET-663) - Link are Broken in README.md
*   [PARQUET-674](https://issues.apache.org/jira/browse/PARQUET-674) - Add an abstraction to get the length of a stream
*   [PARQUET-685](https://issues.apache.org/jira/browse/PARQUET-685) - Deprecated ParquetInputSplit constructor passes parameters in the wrong order.
*   [PARQUET-726](https://issues.apache.org/jira/browse/PARQUET-726) - TestMemoryManager consistently fails
*   [PARQUET-743](https://issues.apache.org/jira/browse/PARQUET-743) - DictionaryFilters can re-use StreamBytesInput when compressed

#### Improvement

*   [PARQUET-77](https://issues.apache.org/jira/browse/PARQUET-77) - Improvements in ByteBuffer read path
*   [PARQUET-99](https://issues.apache.org/jira/browse/PARQUET-99) - Large rows cause unnecessary OOM exceptions
*   [PARQUET-146](https://issues.apache.org/jira/browse/PARQUET-146) - make Parquet compile with java 7 instead of java 6
*   [PARQUET-318](https://issues.apache.org/jira/browse/PARQUET-318) - Remove unnecessary objectmapper from ParquetMetadata
*   [PARQUET-327](https://issues.apache.org/jira/browse/PARQUET-327) - Show statistics in the dump output
*   [PARQUET-341](https://issues.apache.org/jira/browse/PARQUET-341) - Improve write performance with wide schema sparse data
*   [PARQUET-343](https://issues.apache.org/jira/browse/PARQUET-343) - Caching nulls on group node to improve write performance on wide schema sparse data
*   [PARQUET-358](https://issues.apache.org/jira/browse/PARQUET-358) - Add support for temporal logical types to AVRO/Parquet conversion
*   [PARQUET-361](https://issues.apache.org/jira/browse/PARQUET-361) - Add prerelease logic to semantic versions
*   [PARQUET-384](https://issues.apache.org/jira/browse/PARQUET-384) - Add Dictionary Based Filtering to Filter2 API
*   [PARQUET-386](https://issues.apache.org/jira/browse/PARQUET-386) - Printing out the statistics of metadata in parquet-tools
*   [PARQUET-397](https://issues.apache.org/jira/browse/PARQUET-397) - Pig Predicate Pushdown using Filter2 API
*   [PARQUET-421](https://issues.apache.org/jira/browse/PARQUET-421) - Fix mismatch of javadoc names and method parameters in module encoding, column, and hadoop
*   [PARQUET-427](https://issues.apache.org/jira/browse/PARQUET-427) - Push predicates into the whole read path
*   [PARQUET-432](https://issues.apache.org/jira/browse/PARQUET-432) - Complete a todo for method ColumnDescriptor.compareTo()
*   [PARQUET-460](https://issues.apache.org/jira/browse/PARQUET-460) - Parquet files concat tool
*   [PARQUET-480](https://issues.apache.org/jira/browse/PARQUET-480) - Update for Cascading 3.0
*   [PARQUET-484](https://issues.apache.org/jira/browse/PARQUET-484) - Warn when Decimal is stored as INT64 while could be stored as INT32
*   [PARQUET-543](https://issues.apache.org/jira/browse/PARQUET-543) - Remove BoundedInt encodings
*   [PARQUET-585](https://issues.apache.org/jira/browse/PARQUET-585) - Slowly ramp up sizes of int[]s in IntList to keep sizes small when data sets are small
*   [PARQUET-654](https://issues.apache.org/jira/browse/PARQUET-654) - Make record-level filtering optional
*   [PARQUET-668](https://issues.apache.org/jira/browse/PARQUET-668) - Provide option to disable auto crop feature in DumpCommand output
*   [PARQUET-727](https://issues.apache.org/jira/browse/PARQUET-727) - Ensure correct version of thrift is used
*   [PARQUET-740](https://issues.apache.org/jira/browse/PARQUET-740) - Introduce editorconfig

#### New Feature

*   [PARQUET-225](https://issues.apache.org/jira/browse/PARQUET-225) - INT64 support for Delta Encoding
*   [PARQUET-382](https://issues.apache.org/jira/browse/PARQUET-382) - Add a way to append encoded blocks in ParquetFileWriter
*   [PARQUET-429](https://issues.apache.org/jira/browse/PARQUET-429) - Enables predicates collecting their referred columns
*   [PARQUET-548](https://issues.apache.org/jira/browse/PARQUET-548) - Add Java metadata for PageEncodingStats
*   [PARQUET-669](https://issues.apache.org/jira/browse/PARQUET-669) - Allow reading file footers from input streams when writing metadata files

#### Task

*   [PARQUET-392](https://issues.apache.org/jira/browse/PARQUET-392) - Release Parquet-mr 1.9.0
*   [PARQUET-404](https://issues.apache.org/jira/browse/PARQUET-404) - Replace git@github.com.apache for HTTPS URL on dev/README.md to avoid permission issues
*   [PARQUET-696](https://issues.apache.org/jira/browse/PARQUET-696) - Move travis download from google code (defunct) to github

#### Test

*   [PARQUET-355](https://issues.apache.org/jira/browse/PARQUET-355) - Create Integration tests to validate statistics
*   [PARQUET-378](https://issues.apache.org/jira/browse/PARQUET-378) - Add thoroughly parquet test encodings


### Version 1.8.1 ###

#### Bug

*   [PARQUET-331](https://issues.apache.org/jira/browse/PARQUET-331) - Merge script doesn't surface stderr from failed sub processes
*   [PARQUET-336](https://issues.apache.org/jira/browse/PARQUET-336) - ArrayIndexOutOfBounds in checkDeltaByteArrayProblem
*   [PARQUET-337](https://issues.apache.org/jira/browse/PARQUET-337) - binary fields inside map/set/list are not handled in parquet-scrooge
*   [PARQUET-338](https://issues.apache.org/jira/browse/PARQUET-338) - Readme references wrong format of pull request title

#### Improvement

*   [PARQUET-279](https://issues.apache.org/jira/browse/PARQUET-279) - Check empty struct in the CompatibilityChecker util

#### Task

*   [PARQUET-339](https://issues.apache.org/jira/browse/PARQUET-339) - Add Alex Levenson to KEYS file

### Version 1.8.0 ###

#### Bug

*   [PARQUET-151](https://issues.apache.org/jira/browse/PARQUET-151) - Null Pointer exception in parquet.hadoop.ParquetFileWriter.mergeFooters
*   [PARQUET-152](https://issues.apache.org/jira/browse/PARQUET-152) - Encoding issue with fixed length byte arrays
*   [PARQUET-164](https://issues.apache.org/jira/browse/PARQUET-164) - Warn when parquet memory manager kicks in
*   [PARQUET-199](https://issues.apache.org/jira/browse/PARQUET-199) - Add a callback when the MemoryManager adjusts row group size
*   [PARQUET-201](https://issues.apache.org/jira/browse/PARQUET-201) - Column with OriginalType INT_8 failed at filtering
*   [PARQUET-227](https://issues.apache.org/jira/browse/PARQUET-227) - Parquet thrift can write unions that have 0 or more than 1 set value
*   [PARQUET-246](https://issues.apache.org/jira/browse/PARQUET-246) - ArrayIndexOutOfBoundsException with Parquet write version v2
*   [PARQUET-251](https://issues.apache.org/jira/browse/PARQUET-251) - Binary column statistics error when reuse byte[] among rows
*   [PARQUET-252](https://issues.apache.org/jira/browse/PARQUET-252) - parquet scrooge support should support nested container type
*   [PARQUET-254](https://issues.apache.org/jira/browse/PARQUET-254) - Wrong exception message for unsupported INT96 type
*   [PARQUET-269](https://issues.apache.org/jira/browse/PARQUET-269) - Restore scrooge-maven-plugin to 3.17.0 or greater
*   [PARQUET-284](https://issues.apache.org/jira/browse/PARQUET-284) - Should use ConcurrentHashMap instead of HashMap in ParquetMetadataConverter
*   [PARQUET-285](https://issues.apache.org/jira/browse/PARQUET-285) - Implement nested types write rules in parquet-avro
*   [PARQUET-287](https://issues.apache.org/jira/browse/PARQUET-287) - Projecting unions in thrift causes TExceptions in deserializatoin
*   [PARQUET-296](https://issues.apache.org/jira/browse/PARQUET-296) - Set master branch version back to 1.8.0-SNAPSHOT
*   [PARQUET-297](https://issues.apache.org/jira/browse/PARQUET-297) - created_by in file meta data doesn't contain parquet library version
*   [PARQUET-314](https://issues.apache.org/jira/browse/PARQUET-314) - Fix broken equals implementation(s)
*   [PARQUET-316](https://issues.apache.org/jira/browse/PARQUET-316) - Run.sh is broken in parquet-benchmarks
*   [PARQUET-317](https://issues.apache.org/jira/browse/PARQUET-317) - writeMetaDataFile crashes when a relative root Path is used
*   [PARQUET-320](https://issues.apache.org/jira/browse/PARQUET-320) - Restore semver checks
*   [PARQUET-324](https://issues.apache.org/jira/browse/PARQUET-324) - row count incorrect if data file has more than 2^31 rows
*   [PARQUET-325](https://issues.apache.org/jira/browse/PARQUET-325) - Do not target row group sizes if padding is set to 0
*   [PARQUET-329](https://issues.apache.org/jira/browse/PARQUET-329) - ThriftReadSupport#THRIFT_COLUMN_FILTER_KEY was removed (incompatible change)

#### Improvement

*   [PARQUET-175](https://issues.apache.org/jira/browse/PARQUET-175) - Allow setting of a custom protobuf class when reading parquet file using parquet-protobuf.
*   [PARQUET-223](https://issues.apache.org/jira/browse/PARQUET-223) - Add Map and List builiders
*   [PARQUET-245](https://issues.apache.org/jira/browse/PARQUET-245) - Travis CI runs tests even if build fails
*   [PARQUET-248](https://issues.apache.org/jira/browse/PARQUET-248) - Simplify ParquetWriters's constructors
*   [PARQUET-253](https://issues.apache.org/jira/browse/PARQUET-253) - AvroSchemaConverter has confusing Javadoc
*   [PARQUET-259](https://issues.apache.org/jira/browse/PARQUET-259) - Support Travis CI in parquet-cpp
*   [PARQUET-264](https://issues.apache.org/jira/browse/PARQUET-264) - Update README docs for graduation
*   [PARQUET-266](https://issues.apache.org/jira/browse/PARQUET-266) - Add support for lists of primitives to Pig schema converter
*   [PARQUET-272](https://issues.apache.org/jira/browse/PARQUET-272) - Updates docs decscription to match data model
*   [PARQUET-274](https://issues.apache.org/jira/browse/PARQUET-274) - Updates URLs to link against the apache user instead of Parquet on github
*   [PARQUET-276](https://issues.apache.org/jira/browse/PARQUET-276) - Updates CONTRIBUTING file with new repo info
*   [PARQUET-286](https://issues.apache.org/jira/browse/PARQUET-286) - Avro object model should use Utf8
*   [PARQUET-288](https://issues.apache.org/jira/browse/PARQUET-288) - Add dictionary support to Avro converters
*   [PARQUET-289](https://issues.apache.org/jira/browse/PARQUET-289) - Allow object models to extend the ParquetReader builders
*   [PARQUET-290](https://issues.apache.org/jira/browse/PARQUET-290) - Add Avro data model to the reader builder
*   [PARQUET-306](https://issues.apache.org/jira/browse/PARQUET-306) - Improve alignment between row groups and HDFS blocks
*   [PARQUET-308](https://issues.apache.org/jira/browse/PARQUET-308) - Add accessor to ParquetWriter to get current data size
*   [PARQUET-309](https://issues.apache.org/jira/browse/PARQUET-309) - Remove unnecessary compile dependency on parquet-generator
*   [PARQUET-321](https://issues.apache.org/jira/browse/PARQUET-321) - Set the HDFS padding default to 8MB
*   [PARQUET-327](https://issues.apache.org/jira/browse/PARQUET-327) - Show statistics in the dump output

#### New Feature

*   [PARQUET-229](https://issues.apache.org/jira/browse/PARQUET-229) - Make an alternate, stricter thrift column projection API
*   [PARQUET-243](https://issues.apache.org/jira/browse/PARQUET-243) - Add avro-reflect support

#### Task

*   [PARQUET-262](https://issues.apache.org/jira/browse/PARQUET-262) - When 1.7.0 is released, restore semver plugin config
*   [PARQUET-292](https://issues.apache.org/jira/browse/PARQUET-292) - Release Parquet 1.8.0

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
