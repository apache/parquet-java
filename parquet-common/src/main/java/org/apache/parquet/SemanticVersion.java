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

/**
 * Very basic semver parser, only pays attention to major, minor, and patch numbers.
 * Attempts to do a little bit of validation that the version string is valid, but
 * is not a full implementation of the semver spec.
 *
 * NOTE: compareTo only respects major, minor, and patch (ignores rc numbers, SNAPSHOT, etc)
 */
public final class SemanticVersion implements Comparable<SemanticVersion> {
  // (major).(minor).(patch)[(rc)(rcnum)]?(-(SNAPSHOT))?
  private static final String FORMAT = "^(\\d+)\\.(\\d+)\\.(\\d+)((.*)(\\d+))?(\\-(.*))?$";
  private static final Pattern PATTERN = Pattern.compile(FORMAT);

  public final int major;
  public final int minor;
  public final int patch;

  public SemanticVersion(int major, int minor, int patch) {
    Preconditions.checkArgument(major >= 0, "major must be >= 0");
    Preconditions.checkArgument(minor >= 0, "minor must be >= 0");
    Preconditions.checkArgument(patch >= 0, "patch must be >= 0");

    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  public static SemanticVersion parse(String version) throws SemanticVersionParseException {
    Matcher matcher = PATTERN.matcher(version);

    if (!matcher.matches()) {
      throw new SemanticVersionParseException("" + version + " does not match format " + FORMAT);
    }

    final int major;
    final int minor;
    final int patch;

    try {
      major = Integer.valueOf(matcher.group(1));
      minor = Integer.valueOf(matcher.group(2));
      patch = Integer.valueOf(matcher.group(3));
    } catch (NumberFormatException e) {
      throw new SemanticVersionParseException(e);
    }

    if (major < 0 || minor < 0 || patch < 0) {
      throw new SemanticVersionParseException(
          String.format("major(%d), minor(%d), and patch(%d) must all be >= 0", major, minor, patch));
    }

    return new SemanticVersion(major, minor, patch);
  }

  @Override
  public int compareTo(SemanticVersion o) {
    int cmp;

    cmp = Integer.compare(major, o.major);
    if (cmp != 0) {
      return cmp;
    }

    cmp = Integer.compare(minor, o.minor);
    if (cmp != 0) {
      return cmp;
    }

    return Integer.compare(patch, o.patch);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SemanticVersion that = (SemanticVersion) o;
    return compareTo(that) == 0;
  }

  @Override
  public int hashCode() {
    int result = major;
    result = 31 * result + minor;
    result = 31 * result + patch;
    return result;
  }

  @Override
  public String toString() {
    return major + "." + minor + "." + patch;
  }

  public static class SemanticVersionParseException extends Exception {
    public SemanticVersionParseException() {
      super();
    }

    public SemanticVersionParseException(String message) {
      super(message);
    }

    public SemanticVersionParseException(String message, Throwable cause) {
      super(message, cause);
    }

    public SemanticVersionParseException(Throwable cause) {
      super(cause);
    }
  }
}
