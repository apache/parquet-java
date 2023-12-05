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

package org.apache.parquet.cli.csv;

import javax.annotation.concurrent.Immutable;
import org.apache.commons.text.StringEscapeUtils;

@Immutable
public class CSVProperties {

  public static final String DEFAULT_CHARSET = "utf8";
  public static final String DEFAULT_DELIMITER = ",";
  public static final String DEFAULT_QUOTE = "\"";
  public static final String DEFAULT_ESCAPE = "\\";
  public static final String DEFAULT_HAS_HEADER = "false";
  public static final int DEFAULT_LINES_TO_SKIP = 0;

  // configuration
  public final String charset;
  public final String delimiter;
  public final String quote;
  public final String escape;
  public final String header;
  public final boolean useHeader;
  public final int linesToSkip;

  private CSVProperties(
      String charset,
      String delimiter,
      String quote,
      String escape,
      String header,
      boolean useHeader,
      int linesToSkip) {
    this.charset = charset;
    this.delimiter = delimiter;
    this.quote = quote;
    this.escape = escape;
    this.header = header;
    this.useHeader = useHeader;
    this.linesToSkip = linesToSkip;
  }

  public static class Builder {
    private String charset = DEFAULT_CHARSET;
    private String delimiter = DEFAULT_DELIMITER;
    private String quote = DEFAULT_QUOTE;
    private String escape = DEFAULT_ESCAPE;
    private boolean useHeader = Boolean.parseBoolean(DEFAULT_HAS_HEADER);
    private int linesToSkip = DEFAULT_LINES_TO_SKIP;
    private String header = null;

    private static String unescapeJava(String str) {
      // StringEscapeUtils removes the single escape character
      if (str == "\\") {
        return str;
      }
      return StringEscapeUtils.unescapeJava(str);
    }

    public Builder charset(String charset) {
      this.charset = charset;
      return this;
    }

    public Builder delimiter(String delimiter) {
      this.delimiter = unescapeJava(delimiter);
      return this;
    }

    public Builder quote(String quote) {
      this.quote = unescapeJava(quote);
      return this;
    }

    public Builder escape(String escape) {
      this.escape = unescapeJava(escape);
      return this;
    }

    public Builder header(String header) {
      this.header = header;
      return this;
    }

    public Builder hasHeader() {
      this.useHeader = true;
      return this;
    }

    public Builder hasHeader(boolean hasHeader) {
      this.useHeader = hasHeader;
      return this;
    }

    public Builder linesToSkip(int linesToSkip) {
      this.linesToSkip = linesToSkip;
      return this;
    }

    public CSVProperties build() {
      return new CSVProperties(charset, delimiter, quote, escape, header, useHeader, linesToSkip);
    }
  }
}
