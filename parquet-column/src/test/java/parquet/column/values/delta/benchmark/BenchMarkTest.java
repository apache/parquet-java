package parquet.column.values.delta.benchmark;


import parquet.column.values.ValuesWriter;

public abstract class BenchMarkTest {
  public static int[] data;

  protected void runWriteTest(ValuesWriter writer){
    int pageCount = 10;
    double avg = 0.0;
    for (int i = 0; i < pageCount ; i++) {
      writer.reset();
      long startTime = System.nanoTime();
      for(int item:data){
        writer.writeInteger(item);
      }
      long endTime = System.nanoTime();
      long duration = endTime - startTime;
      avg += (double) duration / pageCount;
    }

    System.out.println("size is "+writer.getBytes().size());
  }

}
