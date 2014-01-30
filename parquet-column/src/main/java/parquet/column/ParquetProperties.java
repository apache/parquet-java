package parquet.column;

import static parquet.Log.INFO;

import java.util.Properties;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesWriter;
import parquet.column.values.boundedint.DevNullValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import parquet.column.values.plain.BooleanPlainValuesWriter;
import parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
/**
 * This class represents all the configurable Parquet properties.
 * 
 * @author amokashi
 *
 */
public class ParquetProperties {
  
  private static final Log LOG = Log.getLog(ParquetProperties.class);

  public static final String BLOCK_SIZE           = "parquet.block.size";
  public static final String PAGE_SIZE            = "parquet.page.size";
  public static final String DICTIONARY_PAGE_SIZE = "parquet.dictionary.page.size";
  
  public static final String COMPRESSION          = "parquet.compression";
  public static final String ENABLE_DICTIONARY    = "parquet.enable.dictionary";
  public static final String VALIDATION           = "parquet.validation";
  public static final String WRITER_VERSION       = "parquet.writer.version";

  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;

  public enum WriterVersion {
    PARQUET_1_0 ("v1"),
    PARQUET_2_0 ("v2");
    
    private final String shortName;

    WriterVersion(String shortname) {
      this.shortName = shortname;
    }
    
    public static WriterVersion fromString(String name) {
      for(WriterVersion v : WriterVersion.values()) {
        if (v.shortName.equals(name)) {
          return v;
        }
      }
      // Throws IllegalArgumentException if name does not exact match with enum name
      return WriterVersion.valueOf(name);
    }
  }

  private int blockSize;
  private int pageSize;
  private int dictionaryPageSize;
  private WriterVersion writerVersion;
  private boolean enableDictionary;
  private boolean isValidating;
  
  public ParquetProperties(Properties props) {
    this(Integer.parseInt(props.getProperty(BLOCK_SIZE, String.valueOf(DEFAULT_BLOCK_SIZE))),
        Integer.parseInt(props.getProperty(PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE))), 
        Integer.parseInt(props.getProperty(DICTIONARY_PAGE_SIZE, String.valueOf(DEFAULT_PAGE_SIZE))), 
        WriterVersion.fromString(props.getProperty(WRITER_VERSION, WriterVersion.PARQUET_1_0.toString())), 
        Boolean.parseBoolean(props.getProperty(ENABLE_DICTIONARY, "true")), 
        Boolean.parseBoolean(props.getProperty(VALIDATION, "false")));
  }

  private ParquetProperties(int blockSize, int pageSize, int dictPageSize, WriterVersion writerVersion, boolean enableDict, boolean validating) {
    this.blockSize = blockSize;
    if (INFO) LOG.info("Parquet block size to " + blockSize);
    this.pageSize = pageSize;
    if (INFO) LOG.info("Parquet page size to " + pageSize);
    this.dictionaryPageSize = dictPageSize;
    if (INFO) LOG.info("Parquet dictionary page size to " + dictionaryPageSize);
    
    this.writerVersion = writerVersion;
    if (INFO) LOG.info("Writer version is: " + writerVersion);
    
    this.enableDictionary = enableDict;
    if (INFO) LOG.info("Dictionary is " + (enableDictionary ? "on" : "off"));
    this.isValidating = validating;
    if (INFO) LOG.info("Validation is " + (validating ? "on" : "off"));
  }
  
  public static ValuesWriter getColumnDescriptorValuesWriter(int maxLevel,  int initialSizePerCol) {
    if (maxLevel == 0) {
      return new DevNullValuesWriter();
    } else {
      return new RunLengthBitPackingHybridValuesWriter(
          BytesUtils.getWidthFromMaxInt(maxLevel), initialSizePerCol);
    }
  }

  public ValuesWriter getValuesWriter(ColumnDescriptor path, int initialSizePerCol) {
    switch (path.getType()) {
    case BOOLEAN:
      if(writerVersion == WriterVersion.PARQUET_1_0) {
        return new BooleanPlainValuesWriter();
      } else if (writerVersion == WriterVersion.PARQUET_2_0) {
        return new RunLengthBitPackingHybridValuesWriter(1, initialSizePerCol);
      }
      break;
    case BINARY:
      if(enableDictionary) {
        return new PlainBinaryDictionaryValuesWriter(dictionaryPageSize, initialSizePerCol);
      } else {
        if (writerVersion == WriterVersion.PARQUET_1_0) {
          return new PlainValuesWriter(initialSizePerCol);
        } else if (writerVersion == WriterVersion.PARQUET_2_0) {
          return new DeltaByteArrayWriter(initialSizePerCol);
        } 
      }
      break;
    case INT32:
      if(enableDictionary) {
        return new PlainIntegerDictionaryValuesWriter(dictionaryPageSize, initialSizePerCol);
      } else {
        if(writerVersion == WriterVersion.PARQUET_1_0) {
          return new PlainValuesWriter(initialSizePerCol);
        } else if(writerVersion == WriterVersion.PARQUET_2_0) {
          return new DeltaBinaryPackingValuesWriter(initialSizePerCol);
        }
      }
      break;
    case INT64:
      if(enableDictionary) {
        return new PlainLongDictionaryValuesWriter(dictionaryPageSize, initialSizePerCol);
      } else {
        return new PlainValuesWriter(initialSizePerCol);
      }
    case DOUBLE:
      if(enableDictionary) {
        return new PlainDoubleDictionaryValuesWriter(dictionaryPageSize, initialSizePerCol);
      } else {
        return new PlainValuesWriter(initialSizePerCol);
      }
    case FLOAT:
      if(enableDictionary) {
        return new PlainFloatDictionaryValuesWriter(dictionaryPageSize, initialSizePerCol);
      } else {
        return new PlainValuesWriter(initialSizePerCol);
      }
    case FIXED_LEN_BYTE_ARRAY:
      return new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSizePerCol);
    default:
      return new PlainValuesWriter(initialSizePerCol);
    }
    return null;
  }

  public WriterVersion getWriterVersion() {
    return writerVersion;
  }

  public boolean isEnableDictionary() {
    return enableDictionary;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getDictionaryPageSize() {
    return dictionaryPageSize;
  }

  public boolean isValidating() {
    return isValidating;
  }
}
