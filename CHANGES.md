# Parquet #

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
