package redelm.pig;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import redelm.column.ColumnDescriptor;
import redelm.schema.MessageType;

import org.apache.hadoop.fs.FSDataOutputStream;

public class RedelmFileWriter {
  private static final byte[] magic = {82, 101, 100, 32, 69, 108, 109, 32}; // "Red Elm "
  private static final int currentVersion = 1;
  private final MessageType schema;
  private final FSDataOutputStream out;
  private BlockMetaData currentBlock;
  private ColumnMetaData currentColumn;
  private int currentRecordCount;
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

  public RedelmFileWriter(MessageType schema, FSDataOutputStream out) {
    super();
    this.schema = schema;
    this.out = out;
  }

  public void start() throws IOException {
    out.write(magic);
  }

  public void startBlock() throws IOException {
    currentBlock = new BlockMetaData(out.getPos());
  }

  public void startColumn(ColumnDescriptor descriptor) throws IOException {
    currentColumn = new ColumnMetaData(out.getPos(), descriptor.getPath(), descriptor.getType());
    currentRecordCount = 0;
  }

  public void writeData(byte[] data, int recordCount) throws IOException {
    out.write(data);
    currentRecordCount =+ recordCount;
  }

  public void endColumn() throws IOException {
    currentColumn.setEndIndex(out.getPos());
    currentColumn.setRecordCount(currentRecordCount);
    currentBlock.addColumn(currentColumn);
    currentColumn = null;
  }

  public void endBlock() throws IOException {
    currentBlock.setEndIndex(out.getPos());
    currentBlock.setRecordCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
  }

  public void end() throws IOException {
    long footerIndex = out.getPos();
    out.writeInt(currentVersion);
    Footer footer = new Footer(schema.toString(), blocks);
//    out.writeUTF(Footer.toJSON(footer));
    // lazy: use serialization
    // TODO: change that
    new ObjectOutputStream(out).writeObject(footer);
    out.writeLong(footerIndex);
    out.write(magic);
    out.close();
  }

}
