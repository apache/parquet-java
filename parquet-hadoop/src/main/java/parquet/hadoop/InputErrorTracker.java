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
package parquet.hadoop;


import org.apache.hadoop.conf.Configuration;

import parquet.Log;
import parquet.io.ParquetDecodingException;

// Essentially taken from:
// https://github.com/twitter/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/mapreduce/input/LzoRecordReader.java#L124

/**
 * Tracks number of of errors in input and throws a Runtime exception
 * if the rate of errors crosses a limit.<p>
 *
 * The intention is to skip over very rare file corruption or incorrect
 * input, but catch programmer errors (incorrect format, or incorrect
 * deserializers etc).
 */
public class InputErrorTracker {

  /* Tolerated percent bad records */
  public static final String BAD_RECORD_THRESHOLD_CONF_KEY = "parquet.read.bad.record.threshold";

  /* Error out only after threshold rate is reached and we have see this many errors */
  public static final String BAD_RECORD_MIN_COUNT_CONF_KEY = "parquet.read.bad.record.min";

  /* Error out only after threshold rate is reached and we have see this many errors */
  public static final String BAD_RECORD_ONLY_CHECK_IN_CLOSE_CONF_KEY = // long name is ok, not usually explicitly set.
      "parquet.read.bad.record.check.only.in.close";

  private static final Log LOG = Log.getLog(InputErrorTracker.class);

  private static final float DEFAULT_THRESHOLD =  0f;
  private static final long DEFAULT_MIN_ERRORS = 0;
  private static final boolean DEFAULT_CHECK_AT_CLOSE = false;

  private long numRecords;
  private long numErrors;
  private Throwable lastError;

  private final double errorThreshold; // max fraction of errors allowed
  private final long minErrors; // throw error only after this many errors
  private final boolean checkOnlyInClose;

  public InputErrorTracker() {
    this(DEFAULT_THRESHOLD, DEFAULT_MIN_ERRORS, DEFAULT_CHECK_AT_CLOSE);
  }

  public InputErrorTracker(Configuration conf) {
    this(
        conf.getFloat(BAD_RECORD_THRESHOLD_CONF_KEY, DEFAULT_THRESHOLD),
        conf.getLong(BAD_RECORD_MIN_COUNT_CONF_KEY, DEFAULT_MIN_ERRORS),
        conf.getBoolean(BAD_RECORD_ONLY_CHECK_IN_CLOSE_CONF_KEY, DEFAULT_CHECK_AT_CLOSE)
     );
  }

  public InputErrorTracker(double errorThreshold, long minErrors, boolean checkOnlyInClose) {
    this.errorThreshold = errorThreshold;
    this.minErrors = minErrors;
    this.checkOnlyInClose = checkOnlyInClose;
    numRecords = 0;
    numErrors = 0;
  }

  public void incRecords() {
    numRecords++;
  }

  public void incErrors(Throwable cause) throws ParquetDecodingException {
    this.lastError = cause;
    numErrors++;
    if (numErrors > numRecords) {
      // incorrect use of this class
      throw new ParquetDecodingException("Forgot to invoke incRecords()?");
    }

    LOG.warn("Error while reading an input record ("
        + numErrors + " out of " + numRecords + " so far ): ", cause);

    if (!checkOnlyInClose) {
      checkErrorThreshold(cause);
    }
  }

  public void close() throws ParquetDecodingException {
    if (numErrors > 0) {
      checkErrorThreshold(null);
    }
  }

  private void checkErrorThreshold(Throwable cause) throws ParquetDecodingException {
    if (cause == null) {
      cause = this.lastError;
    }

    if (numErrors > 0 && errorThreshold <= 0) { // no errors are tolerated
      throw new ParquetDecodingException("error while reading input records", cause);
    }

    double errRate = numErrors/(double)numRecords;

    // will always excuse the first error. We can decide if single
    // error crosses threshold inside close() if we want to.
    if (numErrors >= minErrors  && errRate > errorThreshold) {
      LOG.error(numErrors + " out of " + numRecords
          + " crosses configured threshold (" + errorThreshold + ")");
      throw new ParquetDecodingException("error rate while reading input records crossed threshold", cause);
    }
  }
}
