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
package org.apache.parquet;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.parquet.Preconditions.checkArgument;

/**
 * Parses a parquet Version string
 */
public class VersionParser {
  // example: parquet-mr version 1.8.0rc2-SNAPSHOT (build ddb469afac70404ea63b72ed2f07a911a8592ff7)
  public static final String FORMAT = "(.+) version (.+) \\(build (.+)\\)";
  public static final Pattern PATTERN = Pattern.compile(FORMAT);

  public static class ParsedVersion {
    public final String application;
    public final String semver;
    public final String appBuildHash;

    public ParsedVersion(String application, String semver, String appBuildHash) {
      checkArgument(!Strings.isNullOrEmpty(application), "application cannont be null or empty");
      checkArgument(!Strings.isNullOrEmpty(semver), "semver cannont be null or empty");
      checkArgument(!Strings.isNullOrEmpty(appBuildHash), "appBuildHash cannont be null or empty");
      this.application = application;
      this.semver = semver;
      this.appBuildHash = appBuildHash;
    }
  }

  public static ParsedVersion parse(String createdBy) {
    Matcher matcher = PATTERN.matcher(createdBy);
    checkArgument(matcher.matches(), "Could not parse created_by: " + createdBy + " using format: " + FORMAT);
    return new ParsedVersion(matcher.group(1), matcher.group(2), matcher.group(3));
  }

}
