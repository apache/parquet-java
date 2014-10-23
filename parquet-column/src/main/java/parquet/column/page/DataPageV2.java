package parquet.column.page;

import java.util.Map;

import parquet.Ints;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.statistics.Statistics;

public class DataPageV2 extends DataPage {

  /**
   * @param rowCount
   * @param nullCount
   * @param valueCount
   * @param repetitionLevels RLE encoded repetition levels
   * @param definitionLevels RLE encoded definition levels
   * @param dataEncoding encoding for the data
   * @param data data encoded with dataEncoding
   * @param statistics optional statistics for this page
   * @param metadata optional free-form key values
   * @return an uncompressed page
   */
  public static DataPageV2 uncompressed(
      int rowCount, int nullCount, int valueCount,
      BytesInput repetitionLevels, BytesInput definitionLevels,
      Encoding dataEncoding, BytesInput data,
      Statistics<?> statistics,
      Map<String, String> metadata) {
    return new DataPageV2(
        rowCount, nullCount, valueCount,
        repetitionLevels, definitionLevels,
        dataEncoding, data,
        Ints.checkedCast(repetitionLevels.size() + definitionLevels.size() + data.size()),
        statistics,
        metadata,
        false);
  }

  /**
   * @param rowCount
   * @param nullCount
   * @param valueCount
   * @param repetitionLevels RLE encoded repetition levels
   * @param definitionLevels RLE encoded definition levels
   * @param dataEncoding encoding for the data
   * @param data data encoded with dataEncoding and compressed
   * @param uncompressedSize total size uncompressed (rl + dl + data)
   * @param statistics optional statistics for this page
   * @param metadata optional free-form key values
   * @return a compressed page
   */
  public static DataPageV2 compressed(
      int rowCount, int nullCount, int valueCount,
      BytesInput repetitionLevels, BytesInput definitionLevels,
      Encoding dataEncoding, BytesInput data,
      int uncompressedSize,
      Statistics<?> statistics,
      Map<String, String> metadata) {
    return new DataPageV2(
        rowCount, nullCount, valueCount,
        repetitionLevels, definitionLevels,
        dataEncoding, data,
        uncompressedSize,
        statistics,
        metadata,
        true);
  }

  private final int rowCount;
  private final int nullCount;
  private final int valueCount;
  private final BytesInput repetitionLevels;
  private final BytesInput definitionLevels;
  private final Encoding dataEncoding;
  private final BytesInput data;
  private final Statistics<?> statistics;
  private final Map<String, String> metadata;
  private final boolean isCompressed;

  public DataPageV2(
      int rowCount, int nullCount, int valueCount,
      BytesInput repetitionLevels, BytesInput definitionLevels,
      Encoding dataEncoding, BytesInput data,
      int uncompressedSize,
      Statistics<?> statistics,
      Map<String, String> metadata,
      boolean isCompressed) {
    super(Ints.checkedCast(repetitionLevels.size() + definitionLevels.size() + data.size()), uncompressedSize);
    this.rowCount = rowCount;
    this.nullCount = nullCount;
    this.valueCount = valueCount;
    this.repetitionLevels = repetitionLevels;
    this.definitionLevels = definitionLevels;
    this.dataEncoding = dataEncoding;
    this.data = data;
    this.statistics = statistics;
    this.metadata = metadata;
    this.isCompressed = isCompressed;
  }

  public int getRowCount() {
    return rowCount;
  }

  public int getNullCount() {
    return nullCount;
  }

  public int getValueCount() {
    return valueCount;
  }

  public BytesInput getRepetitionLevels() {
    return repetitionLevels;
  }

  public BytesInput getDefinitionLevels() {
    return definitionLevels;
  }

  public Encoding getDataEncoding() {
    return dataEncoding;
  }

  public BytesInput getData() {
    return data;
  }

  public Statistics<?> getStatistics() {
    return statistics;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  @Override
  public <T> T accept(Visitor<T> visitor) {
    return visitor.visit(this);
  }

}
