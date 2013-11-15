package parquet.column.values.delta.benchmark;


import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.junit.BeforeClass;
import org.junit.Test;
import parquet.column.values.ValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

import java.util.Random;

@AxisRange(min = 0, max = 2)
@BenchmarkMethodChart(filePrefix = "benchmark-encoding-writing-random-small")
public class SmallRangeWritingBenchmarkTest extends RandomWritingBenchmarkTest {
  @BeforeClass
  public static void prepare() {
    Random random=new Random();
    data = new int[100000 * blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(2) - 1;
    }
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 2)
  @Test
  public void writeRLEWithSmallBitWidthTest(){
    ValuesWriter writer = new RunLengthBitPackingHybridValuesWriter(2,100);
    runWriteTest(writer);
  }
}
