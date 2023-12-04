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
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.RecordMaterializer.RecordMaterializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Essentially taken from:
// https://github.com/twitter/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/mapreduce/input/LzoRecordReader.java#L124

/**
 * Tracks number of records that cannot be materialized and throws ParquetDecodingException
 * if the rate of errors crosses a limit.<p> These types of errors are meant
 * to be recoverable record conversion errors, such as a union missing a value, or schema
 * mismatch and so on. It's not meant to recover from corruptions in the parquet
 * columns themselves.
 * <p>
 * The intention is to skip over very rare file corruption or bugs where
 * the write path has allowed invalid records into the file, but still catch large
 * numbers of failures. Not turned on by default (by default, no errors are tolerated).
 */
public class UnmaterializableRecordCounter {

  /* Tolerated percent bad records */
  public static final String BAD_RECORD_THRESHOLD_CONF_KEY = "parquet.read.bad.record.threshold";

  private static final Logger LOG = LoggerFactory.getLogger(UnmaterializableRecordCounter.class);

  private static final float DEFAULT_THRESHOLD = 0f;

  private long numErrors;

  private final double errorThreshold; // max fraction of errors allowed
  private final long totalNumRecords; // how many records are we going to see total?

  public UnmaterializableRecordCounter(Configuration conf, long totalNumRecords) {
    this(conf.getFloat(BAD_RECORD_THRESHOLD_CONF_KEY, DEFAULT_THRESHOLD), totalNumRecords);
  }

  public UnmaterializableRecordCounter(ParquetReadOptions options, long totalNumRecords) {
    this(getFloat(options, BAD_RECORD_THRESHOLD_CONF_KEY, DEFAULT_THRESHOLD), totalNumRecords);
  }

  public UnmaterializableRecordCounter(double errorThreshold, long totalNumRecords) {
    this.errorThreshold = errorThreshold;
    this.totalNumRecords = totalNumRecords;
    numErrors = 0;
  }

  public void incErrors(RecordMaterializationException cause) throws ParquetDecodingException {
    numErrors++;

    LOG.warn(
        String.format("Error while reading an input record (%s out of %s): ", numErrors, totalNumRecords),
        cause);

    if (numErrors > 0 && errorThreshold <= 0) { // no errors are tolerated
      throw new ParquetDecodingException("Error while decoding records", cause);
    }

    double errRate = numErrors / (double) totalNumRecords;

    if (errRate > errorThreshold) {
      String message = String.format(
          "Decoding error rate of at least %s/%s crosses configured threshold of %s",
          numErrors, totalNumRecords, errorThreshold);
      LOG.error(message);
      throw new ParquetDecodingException(message, cause);
    }
  }

  private static float getFloat(ParquetReadOptions options, String key, float defaultValue) {
    String value = options.getProperty(key);
    if (value != null) {
      return Float.parseFloat(value);
    } else {
      return defaultValue;
    }
  }
}
