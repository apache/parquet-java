package parquet.column.values.dictionary;

import java.util.ArrayList;
import java.util.List;

public class IntList {

  private static final int SLAB_SIZE = 64 * 1024;

  public static class IntIterator {

    private final int[][] slabs;
    private int current;
    private final int count;

    public IntIterator(int[][] slabs, int count) {
      this.slabs = slabs;
      this.count = count;
    }

    public boolean hasNext() {
      return current < count;
    }

    public int next() {
      final int result = slabs[current / SLAB_SIZE][current % SLAB_SIZE];
      ++ current;
      return result;
    }

  }

  private List<int[]> slabs = new ArrayList<int[]>();
  private int[] currentSlab;
  private int currentSlabPos;

  public IntList() {
    initSlab();
  }

  private void initSlab() {
    currentSlab = new int[SLAB_SIZE];
    currentSlabPos = 0;
  }

  public void add(int i) {
    if (currentSlabPos == currentSlab.length) {
      slabs.add(currentSlab);
      initSlab();
    }
    currentSlab[currentSlabPos] = i;
    ++ currentSlabPos;
  }

  public IntIterator iterator() {
    int[][] itSlabs = new int[slabs.size() + 1][];
    for (int i = 0; i < slabs.size(); i++) {
      itSlabs[i] = slabs.get(i);
    }
    itSlabs[slabs.size()] = currentSlab;
    return new IntIterator(itSlabs, SLAB_SIZE * slabs.size() + currentSlabPos);
  }

  public int size() {
    return SLAB_SIZE * slabs.size() + currentSlabPos;
  }

}
