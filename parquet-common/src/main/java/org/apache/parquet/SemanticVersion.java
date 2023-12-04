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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Very basic semver parser, only pays attention to major, minor, and patch numbers.
 * Attempts to do a little bit of validation that the version string is valid, but
 * is not a full implementation of the semver spec.
 * <p>
 * NOTE: compareTo only respects major, minor, patch, and whether this is a
 * prerelease version. All prerelease versions are considered equivalent.
 */
public final class SemanticVersion implements Comparable<SemanticVersion> {
  // this is slightly more permissive than the semver format:
  // * it allows a pattern after patch and before -prerelease or +buildinfo
  private static final String FORMAT =
      // major  . minor  .patch   ???       - prerelease.x + build info
      "^(\\d+)\\.(\\d+)\\.(\\d+)([^-+]*)?(?:-([^+]*))?(?:\\+(.*))?$";
  private static final Pattern PATTERN = Pattern.compile(FORMAT);

  public final int major;
  public final int minor;
  public final int patch;
  // this is part of the public API and can't be renamed. it is misleading
  // because it actually signals that there is an unknown component
  public final boolean prerelease;
  public final String unknown;
  public final Prerelease pre;
  public final String buildInfo;

  public SemanticVersion(int major, int minor, int patch) {
    Preconditions.checkArgument(major >= 0, "major must be >= 0");
    Preconditions.checkArgument(minor >= 0, "minor must be >= 0");
    Preconditions.checkArgument(patch >= 0, "patch must be >= 0");

    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.prerelease = false;
    this.unknown = null;
    this.pre = null;
    this.buildInfo = null;
  }

  public SemanticVersion(int major, int minor, int patch, boolean hasUnknown) {
    Preconditions.checkArgument(major >= 0, "major must be >= 0");
    Preconditions.checkArgument(minor >= 0, "minor must be >= 0");
    Preconditions.checkArgument(patch >= 0, "patch must be >= 0");

    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.prerelease = hasUnknown;
    this.unknown = null;
    this.pre = null;
    this.buildInfo = null;
  }

  public SemanticVersion(int major, int minor, int patch, String unknown, String pre, String buildInfo) {
    Preconditions.checkArgument(major >= 0, "major must be >= 0");
    Preconditions.checkArgument(minor >= 0, "minor must be >= 0");
    Preconditions.checkArgument(patch >= 0, "patch must be >= 0");

    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.prerelease = (unknown != null && !unknown.isEmpty());
    this.unknown = unknown;
    this.pre = (pre != null ? new Prerelease(pre) : null);
    this.buildInfo = buildInfo;
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
      major = Integer.parseInt(matcher.group(1));
      minor = Integer.parseInt(matcher.group(2));
      patch = Integer.parseInt(matcher.group(3));
    } catch (NumberFormatException e) {
      throw new SemanticVersionParseException(e);
    }

    final String unknown = matcher.group(4);
    final String prerelease = matcher.group(5);
    final String buildInfo = matcher.group(6);

    if (major < 0 || minor < 0 || patch < 0) {
      throw new SemanticVersionParseException(
          String.format("major(%d), minor(%d), and patch(%d) must all be >= 0", major, minor, patch));
    }

    return new SemanticVersion(major, minor, patch, unknown, prerelease, buildInfo);
  }

  @Override
  public int compareTo(SemanticVersion o) {
    int cmp;

    cmp = compareIntegers(major, o.major);
    if (cmp != 0) {
      return cmp;
    }

    cmp = compareIntegers(minor, o.minor);
    if (cmp != 0) {
      return cmp;
    }

    cmp = compareIntegers(patch, o.patch);
    if (cmp != 0) {
      return cmp;
    }

    cmp = compareBooleans(o.prerelease, prerelease);
    if (cmp != 0) {
      return cmp;
    }

    if (pre != null) {
      if (o.pre != null) {
        return pre.compareTo(o.pre);
      } else {
        return -1;
      }
    } else if (o.pre != null) {
      return 1;
    }

    return 0;
  }

  private static int compareIntegers(int x, int y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }

  private static int compareBooleans(boolean x, boolean y) {
    return (x == y) ? 0 : (x ? 1 : -1);
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
    StringBuilder sb = new StringBuilder();
    sb.append(major).append(".").append(minor).append(".").append(patch);
    if (prerelease) {
      sb.append(unknown);
    }
    if (pre != null) {
      sb.append(pre.original);
    }
    if (buildInfo != null) {
      sb.append(buildInfo);
    }
    return sb.toString();
  }

  private static class NumberOrString implements Comparable<NumberOrString> {
    private static final Pattern NUMERIC = Pattern.compile("\\d+");

    private final String original;
    private final boolean isNumeric;
    private final int number;

    public NumberOrString(String numberOrString) {
      this.original = numberOrString;
      this.isNumeric = NUMERIC.matcher(numberOrString).matches();
      if (isNumeric) {
        this.number = Integer.parseInt(numberOrString);
      } else {
        this.number = -1;
      }
    }

    @Override
    public int compareTo(NumberOrString that) {
      // Numeric identifiers always have lower precedence than non-numeric identifiers.
      int cmp = compareBooleans(that.isNumeric, this.isNumeric);
      if (cmp != 0) {
        return cmp;
      }

      if (isNumeric) {
        // identifiers consisting of only digits are compared numerically
        return compareIntegers(this.number, that.number);
      }

      // identifiers with letters or hyphens are compared lexically in ASCII sort order
      return this.original.compareTo(that.original);
    }

    @Override
    public String toString() {
      return original;
    }
  }

  private static class Prerelease implements Comparable<Prerelease> {
    private static final Pattern DOT = Pattern.compile("\\.");

    private final String original;
    private final List<NumberOrString> identifiers = new ArrayList<NumberOrString>();

    public Prerelease(String original) {
      this.original = original;
      for (String identifier : DOT.split(original)) {
        identifiers.add(new NumberOrString(identifier));
      }
    }

    @Override
    public int compareTo(Prerelease that) {
      // A larger set of pre-release fields has a higher precedence than a
      // smaller set, if all of the preceding identifiers are equal
      int size = Math.min(this.identifiers.size(), that.identifiers.size());
      for (int i = 0; i < size; i += 1) {
        int cmp = identifiers.get(i).compareTo(that.identifiers.get(i));
        if (cmp != 0) {
          return cmp;
        }
      }
      return compareIntegers(this.identifiers.size(), that.identifiers.size());
    }

    @Override
    public String toString() {
      return original;
    }
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
