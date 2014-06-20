# Parquet #

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
