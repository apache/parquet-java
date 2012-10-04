package redelm.pig;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import redelm.column.ColumnDescriptor;

import org.apache.hadoop.fs.FSDataInputStream;

public class RedelmFileReader {

  public static final Footer readFooter(FSDataInputStream f, long l) throws IOException {
    long footerIndexIndex = l - 8 - 8;
    f.seek(footerIndexIndex);
    long footerIndex = f.readLong();
    f.seek(footerIndex);
    int version = f.readInt();
    if (version != 1) {
      throw new RuntimeException("unsupported version: "+version);
    }
//    Footer footer = Footer.fromJSON(f.readUTF());
    try {
      return (Footer) new ObjectInputStream(f).readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Can not deserialize footer", e);
    }
  }

  private final List<BlockMetaData> blocks;
  private final FSDataInputStream f;
  private int currentBlock = 0;
  private Set<String> paths = new HashSet<String>();

  public RedelmFileReader(FSDataInputStream f, List<BlockMetaData> blocks, List<String[]> colums) {
    this.f = f;
    this.blocks = blocks;
    for (String[] path : colums) {
      paths.add(Arrays.toString(path));
    }
  }

  public List<ColumnData> readColumns() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    List<ColumnData> result = new ArrayList<ColumnData>();
    BlockMetaData block = blocks.get(currentBlock);
    for (ColumnMetaData mc : block.getColumns()) {
      if (paths.contains(Arrays.toString(mc.getPath()))) {
        byte[] data = new byte[(int)(mc.getEndIndex() - mc.getStartIndex())];
        f.readFully(mc.getStartIndex(), data);
        result.add(new ColumnData(mc.getPath(), data));
      }
    }
    ++currentBlock;
    System.out.println(result);
    return result;
  }

}
