package parquet.thrift.projection;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PathGlobPatternTest {
  @Test
  public void testRecursiveGlob() {
    PathGlobPattern g = new PathGlobPattern("a/**/b");
    assertFalse(g.matches("a/b"));
    assertTrue(g.matches("a/asd/b"));
    assertTrue(g.matches("a/asd/ss/b"));

    g = new PathGlobPattern("a/**");
    assertTrue(g.matches("a/as"));
    assertTrue(g.matches("a/asd/b"));
    assertTrue(g.matches("a/asd/ss/b"));


  }

  @Test
  public void testStandardGlob() {
    PathGlobPattern g = new PathGlobPattern("a/*");
    assertTrue(g.matches("a/as"));
    assertFalse(g.matches("a/asd/b"));
    assertFalse(g.matches("a/asd/ss/b"));

    g = new PathGlobPattern("a/{bb,cc}/d");
    assertTrue(g.matches("a/bb/d"));
    assertTrue(g.matches("a/cc/d"));
    assertFalse(g.matches("a/cc/bb/d"));
    assertFalse(g.matches("a/d"));

  }
}
