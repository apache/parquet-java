package redelm.pig;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.log4j.Logger;

public class RedelmFileReader {
  private static final Logger LOG = Logger.getLogger(RedelmFileReader.class);

  public static final Footer readFooter(FSDataInputStream f, long l) throws IOException {
    long footerIndexIndex = l - 8 - 8;
    LOG.info("reading footer index at " + footerIndexIndex);
    f.seek(footerIndexIndex);
    long footerIndex = f.readLong();
    byte[] magic = new byte[8];
    f.readFully(magic);
    if (!Arrays.equals(RedelmFileWriter.MAGIC, magic)) {
      throw new RuntimeException("Not a Red Elm file");
    }
    LOG.info("read footer index: " + footerIndex);
    f.seek(footerIndex);
    int version = f.readInt();
    if (version != RedelmFileWriter.CURRENT_VERSION) {
      throw new RuntimeException(
          "unsupported version: " + version + ". " +
          "supporting up to " + RedelmFileWriter.CURRENT_VERSION);
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
    long previousReadIndex = 0;
    for (ColumnMetaData mc : block.getColumns()) {
      String pathKey = Arrays.toString(mc.getPath());
      if (paths.contains(pathKey)) {
        byte[] data = new byte[(int)(mc.getEndIndex() - mc.getStartIndex())];
        if (mc.getStartIndex() != previousReadIndex) {
          LOG.info("seeking to next column " + (mc.getStartIndex() - previousReadIndex) + " bytes");
        }
        long t0 = System.currentTimeMillis();
        f.readFully(mc.getStartIndex(), data);
        long t1 = System.currentTimeMillis();
        LOG.info("Read " + data.length + " bytes for column " + pathKey + " in " + (t1 - t0) + " ms: " + (float)(t1 - t0)/data.length + " ms/byte");
        previousReadIndex = mc.getEndIndex();
        result.add(new ColumnData(mc.getPath(), data));
      }
    }
    ++currentBlock;
    return result;
  }

}
