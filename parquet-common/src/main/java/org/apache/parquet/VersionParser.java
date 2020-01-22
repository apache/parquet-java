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

import org.apache.parquet.SemanticVersion.SemanticVersionParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.parquet.Preconditions.checkArgument;

/**
 * Parses a parquet Version string Tolerates missing semver and buildhash
 * (semver and build hash may be null)
 */
public class VersionParser {
  // example: parquet-mr version 1.8.0rc2-SNAPSHOT (build
  // ddb469afac70404ea63b72ed2f07a911a8592ff7)
  public static final String FORMAT = "(.*?)\\s+version\\s*(?:([^(]*?)\\s*(?:\\(\\s*build\\s*([^)]*?)\\s*\\))?)?";
  public static final Pattern PATTERN = Pattern.compile(FORMAT);

  public static class ParsedVersion {
    public final String application;
    public final String version;
    public final String appBuildHash;

    private final boolean hasSemver;
    private final SemanticVersion semver;

    public ParsedVersion(String application, String version, String appBuildHash) {
      checkArgument(!Strings.isNullOrEmpty(application), "application cannot be null or empty");
      this.application = application;
      this.version = Strings.isNullOrEmpty(version) ? null : version;
      this.appBuildHash = Strings.isNullOrEmpty(appBuildHash) ? null : appBuildHash;

      SemanticVersion sv;
      boolean hasSemver;
      try {
        sv = SemanticVersion.parse(version);
        hasSemver = true;
      } catch (RuntimeException | SemanticVersionParseException e) {
        sv = null;
        hasSemver = false;
      }
      this.semver = sv;
      this.hasSemver = hasSemver;
    }

    public boolean hasSemanticVersion() {
      return hasSemver;
    }

    public SemanticVersion getSemanticVersion() {
      return semver;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      ParsedVersion version = (ParsedVersion) o;

      if (appBuildHash != null ? !appBuildHash.equals(version.appBuildHash) : version.appBuildHash != null)
        return false;
      if (application != null ? !application.equals(version.application) : version.application != null)
        return false;
      if (this.version != null ? !this.version.equals(version.version) : version.version != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = application != null ? application.hashCode() : 0;
      result = 31 * result + (version != null ? version.hashCode() : 0);
      result = 31 * result + (appBuildHash != null ? appBuildHash.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "ParsedVersion(" + "application=" + application + ", semver=" + version + ", appBuildHash=" + appBuildHash
          + ')';
    }
  }

  public static ParsedVersion parse(String createdBy) throws VersionParseException {
    Matcher matcher = PATTERN.matcher(createdBy);

    if (!matcher.matches()) {
      throw new VersionParseException("Could not parse created_by: " + createdBy + " using format: " + FORMAT);
    }

    String application = matcher.group(1);
    String semver = matcher.group(2);
    String appBuildHash = matcher.group(3);

    if (Strings.isNullOrEmpty(application)) {
      throw new VersionParseException("application cannot be null or empty");
    }

    return new ParsedVersion(application, semver, appBuildHash);
  }

  public static class VersionParseException extends Exception {
    public VersionParseException(String message) {
      super(message);
    }
  }

}
