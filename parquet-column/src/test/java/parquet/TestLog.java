package parquet;

import org.junit.Assert;
import org.junit.Test;

public class TestLog {

  @Test
  public void test() {
    // Use a compile time log level of INFO for performance
    Assert.assertFalse("Do not merge in log level DEBUG", Log.DEBUG);
  }
}
