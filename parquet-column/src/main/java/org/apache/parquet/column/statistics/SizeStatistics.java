/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.statistics;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * A structure for capturing metadata for estimating the unencoded,
 * uncompressed size of data written. This is useful for readers to estimate
 * how much memory is needed to reconstruct data in their memory model and for
 * fine-grained filter push down on nested structures (the histograms contained
 * in this structure can help determine the number of nulls at a particular
 * nesting level and maximum length of lists).
 */
public class SizeStatistics {

  private PrimitiveType type;
  /**
   * The number of physical bytes stored for BYTE_ARRAY data values assuming
   * no encoding. This is exclusive of the bytes needed to store the length of
   * each byte array. In other words, this field is equivalent to the `(size
   * of PLAIN-ENCODING the byte array values) - (4 bytes * number of values
   * written)`. To determine unencoded sizes of other types readers can use
   * schema information multiplied by the number of non-null and null values.
   * The number of null/non-null values can be inferred from the histograms
   * below.
   *
   * For example, if a column chunk is dictionary-encoded with dictionary
   * ["a", "bc", "cde"], and a data page contains the indices [0, 0, 1, 2],
   * then this value for that data page should be 7 (1 + 1 + 2 + 3).
   *
   * This field should only be set for types that use BYTE_ARRAY as their
   * physical type.
   */
  private long unencodedByteArrayDataBytes;
  /**
   * When present, there is expected to be one element corresponding to each
   * repetition (i.e. size=max repetition_level+1) where each element
   * represents the number of times the repetition level was observed in the
   * data.
   *
   * This field may be omitted if max_repetition_level is 0 without loss
   * of information.
   */
  private List<Long> repetitionLevelHistogram;
  /**
   * Same as repetition_level_histogram except for definition levels.
   *
   * This field may be omitted if max_definition_level is 0 or 1 without
   * loss of information.
   */
  private List<Long> definitionLevelHistogram;

  /**
   * Whether the statistics has valid value.
   *
   * It is true by default. Only set to false while it fails to merge statistics.
   */
  private boolean hasValue = true;

  /**
   * Builder to create a SizeStatistics.
   */
  public static class Builder {
    private PrimitiveType type;
    private long unencodedByteArrayDataBytes;
    private long[] repetitionLevelHistogram;
    private long[] definitionLevelHistogram;

    /**
     * Create a builder to create a SizeStatistics.
     *
     * @param type physical type of the column associated with this statistics
     * @param maxRepetitionLevel maximum repetition level of the column
     * @param maxDefinitionLevel maximum definition level of the column
     */
    public Builder(PrimitiveType type, int maxRepetitionLevel, int maxDefinitionLevel) {
      this.type = type;
      this.unencodedByteArrayDataBytes = 0L;
      repetitionLevelHistogram = new long[maxRepetitionLevel + 1];
      definitionLevelHistogram = new long[maxDefinitionLevel + 1];
      Arrays.fill(repetitionLevelHistogram, 0L);
      Arrays.fill(definitionLevelHistogram, 0L);
    }

    /**
     * Add repetition and definition level of a value to the statistics.
     * It is called when value is null, or the column is not of BYTE_ARRAY type.
     *
     * @param repetitionLevel repetition level of the value
     * @param definitionLevel definition level of the value
     */
    public void add(int repetitionLevel, int definitionLevel) {
      Preconditions.checkArgument(0 <= repetitionLevel && repetitionLevel < repetitionLevelHistogram.length,
        "repetitionLevel %s is out of range [0, %s]", repetitionLevel, repetitionLevelHistogram.length - 1);
      Preconditions.checkArgument(definitionLevel < definitionLevelHistogram.length,
        "definitionLevel %s is out of range [0, %s]", definitionLevel, definitionLevelHistogram.length - 1);
      repetitionLevelHistogram[repetitionLevel]++;
      definitionLevelHistogram[definitionLevel]++;
    }

    /**
     * Add repetition and definition level of a value to the statistics.
     * It is called when value is null, or the column is not of BYTE_ARRAY type.
     *
     * @param repetitionLevel repetition level of the value
     * @param definitionLevel definition level of the value
     * @param value value of to be added
     */
    public void add(int repetitionLevel, int definitionLevel, Binary value) {
      add(repetitionLevel, definitionLevel);
      if (type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
        unencodedByteArrayDataBytes = Math.addExact(unencodedByteArrayDataBytes, value.length());
      }
    }

    /**
     * Build a SizeStatistics from the builder.
     */
    public SizeStatistics build() {
      return new SizeStatistics(type, unencodedByteArrayDataBytes,
        new LongArrayList(repetitionLevelHistogram), new LongArrayList(definitionLevelHistogram));
    }

  }

  /**
   * Create a builder to create a SizeStatistics.
   *
   * @param type physical type of the column associated with this statistics
   * @param maxRepetitionLevel maximum repetition level of the column
   * @param maxDefinitionLevel maximum definition level of the column
   */
  public static Builder newBuilder(PrimitiveType type, int maxRepetitionLevel, int maxDefinitionLevel) {
    return new Builder(type, maxRepetitionLevel, maxDefinitionLevel);
  }

  /**
   * Create a SizeStatistics.
   *
   * @param type physical type of the column associated with this statistics
   * @param unencodedByteArrayDataBytes number of physical bytes stored for BYTE_ARRAY data values assuming no encoding
   * @param repetitionLevelHistogram histogram for all repetition levels if non-empty
   * @param definitionLevelHistogram histogram for all definition levels if non-empty
   */
  public SizeStatistics(PrimitiveType type,
                        long unencodedByteArrayDataBytes,
                        List<Long> repetitionLevelHistogram,
                        List<Long> definitionLevelHistogram) {
    this.type = type;
    this.unencodedByteArrayDataBytes = unencodedByteArrayDataBytes;
    this.repetitionLevelHistogram = repetitionLevelHistogram;
    this.definitionLevelHistogram = definitionLevelHistogram;
  }

  /**
   * Merge two SizeStatistics of the same column.
   * It is used to merge size statistics from all pages of the same column chunk.
   */
  public void mergeStatistics(SizeStatistics other) {
    if (!hasValue) {
      return;
    }

    // Stop merge if other is invalid.
    if (other == null || !other.isValid()) {
      hasValue = false;
      unencodedByteArrayDataBytes = 0L;
      repetitionLevelHistogram.clear();
      definitionLevelHistogram.clear();
      return;
    }

    Preconditions.checkArgument(type.equals(other.type), "Cannot merge SizeStatistics of different types");
    Preconditions.checkArgument(repetitionLevelHistogram.size() == other.repetitionLevelHistogram.size(),
      "Cannot merge repetitionLevelHistogram with different sizes");
    Preconditions.checkArgument(definitionLevelHistogram.size() == other.definitionLevelHistogram.size(),
      "Cannot merge definitionLevelHistogram with different sizes");
    unencodedByteArrayDataBytes = Math.addExact(unencodedByteArrayDataBytes, other.unencodedByteArrayDataBytes);
    for (int i = 0; i < repetitionLevelHistogram.size(); i++) {
      repetitionLevelHistogram.set(i, repetitionLevelHistogram.get(i) + other.getRepetitionLevelHistogram().get(i));
    }
    for (int i = 0; i < definitionLevelHistogram.size(); i++) {
      definitionLevelHistogram.set(i, definitionLevelHistogram.get(i) + other.getDefinitionLevelHistogram().get(i));
    }
  }

  public PrimitiveType getType() {
    return type;
  }

  /**
   * @return unencoded and uncompressed byte size of the BYTE_ARRAY column, or empty for other types.
   */
  public Optional<Long> getUnencodedByteArrayDataBytes() {
    if (type.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.BINARY) {
      return Optional.empty();
    }
    return Optional.of(unencodedByteArrayDataBytes);
  }

  /**
   * @return repetition level histogram of all levels if not empty.
   */
  public List<Long> getRepetitionLevelHistogram() {
    return repetitionLevelHistogram;
  }

  /**
   * @return definition level histogram of all levels if not empty.
   */
  public List<Long> getDefinitionLevelHistogram() {
    return definitionLevelHistogram;
  }

  /**
   * @return a new independent statistics instance of this class.
   */
  public SizeStatistics copy() {
    return new SizeStatistics(type, unencodedByteArrayDataBytes,
      new LongArrayList(repetitionLevelHistogram), new LongArrayList(definitionLevelHistogram));
  }

  /**
   * @return whether the statistics has valid value.
   */
  public boolean isValid() {
    return hasValue;
  }

}
