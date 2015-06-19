package org.apache.parquet;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test doesn't do much, but it makes sure that the Version class
 * was properly generated, and that it's VERSION_NUMBER field has been
 * populated correctly. The hope is to catch any issues like the version
 * being an empty string or something along those lines.
 */
public class VersionTest {

  private void assertVersionValid(String v) {
    try {
      org.semver.Version.parse(v);
    } catch (RuntimeException e) {
      throw new RuntimeException(v + " is not a valid semver!" , e);
    }
  }

  @Test
  public void testVersion() {
    assertVersionValid(Version.VERSION_NUMBER);
  }

  @Test
  public void testFullVersion() {
    // example: parquet-mr version 1.8.0rc2-SNAPSHOT (build ddb469afac70404ea63b72ed2f07a911a8592ff7)
    String regex = "parquet-mr version (.*) \\(build (.*)\\)";
    Pattern pattern = Pattern.compile(regex);
    Matcher m = pattern.matcher(Version.FULL_VERSION);
    assertTrue(Version.FULL_VERSION + " did not match " + pattern, m.matches());
    assertVersionValid(m.group(0));
    assertEquals(Version.VERSION_NUMBER, m.group(0));
    assertFalse(m.group(1).isEmpty());
  }
}
