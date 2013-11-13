package parquet.column.values.delta;


import org.junit.BeforeClass;

import java.util.Random;

public class SmallRangeWritingBenchmarkTest extends RandomWritingBenchmarkTest{
  @BeforeClass
  public static void prepare() {
    Random random=new Random();
    data = new int[10000 * blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(10) - 20;
    }
  }
}
