package redelm.pig;

import static redelm.Log.INFO;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import redelm.Log;
import redelm.column.BytesOutput;
import redelm.column.ColumnDescriptor;
import redelm.schema.MessageType;

import org.apache.hadoop.fs.FSDataOutputStream;

public class RedelmFileWriter extends BytesOutput {
  public static final byte[] MAGIC = {82, 101, 100, 32, 69, 108, 109, 10}; // "Red Elm\n"
  public static final int CURRENT_VERSION = 1;
  private static final Log LOG = Log.getLog(RedelmFileWriter.class);

  private final MessageType schema;
  private final FSDataOutputStream out;
  private BlockMetaData currentBlock;
  private ColumnMetaData currentColumn;
  private int currentRecordCount;
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

  private enum STATE {
    NOT_STARTED {
      STATE start() {
        return STARTED;
      }
    },
    STARTED {
      STATE startBlock() {
        return BLOCK;
      }
      STATE end() {
        return ENDED;
      }
    },
    BLOCK  {
      STATE startColumn() {
        return COLUMN;
      }
      STATE endBlock() {
        return STARTED;
      }
    },
    COLUMN {
      STATE startRepetition() {
        return REPETITION;
      }
    },
    REPETITION {
      STATE startDefinition() {
        return DEFINITION;
      }
    },
    DEFINITION {
      STATE startData() {
        return DATA;
      }
    },
    DATA {
      STATE endColumn() {
        return BLOCK;
      };
    },
    ENDED;

    STATE start() {throw new IllegalStateException(this.name());}
    STATE startBlock() {throw new IllegalStateException(this.name());}
    STATE startColumn() {throw new IllegalStateException(this.name());}
    STATE startRepetition() {throw new IllegalStateException(this.name());}
    STATE startDefinition() {throw new IllegalStateException(this.name());}
    STATE startData() {throw new IllegalStateException(this.name());}
    STATE endColumn() {throw new IllegalStateException(this.name());}
    STATE endBlock() {throw new IllegalStateException(this.name());}
    STATE end() {throw new IllegalStateException(this.name());}
  }

  private STATE state = STATE.NOT_STARTED;

  public RedelmFileWriter(MessageType schema, FSDataOutputStream out) {
    super();
    this.schema = schema;
    this.out = out;
  }

  public void start() throws IOException {
    state = state.start();
    out.write(MAGIC);
  }

  public void startBlock(int recordCount) throws IOException {
    state = state.startBlock();
    currentBlock = new BlockMetaData(out.getPos());
    currentRecordCount = recordCount;
  }

  public void startColumn(ColumnDescriptor descriptor, int valueCount) throws IOException {
    state = state.startColumn();
    currentColumn = new ColumnMetaData(descriptor.getPath(), descriptor.getType());
    currentColumn.setValueCount(valueCount);
  }

  public void startRepetitionLevels() throws IOException {
    state = state.startRepetition();
    currentColumn.setRepetitionStart(out.getPos());
  }

  public void write(byte[] data, int offset, int length) throws IOException {
    out.write(data, offset, length);
  }

  public void startDefinitionLevels() throws IOException {
    state = state.startDefinition();
    currentColumn.setDefinitionStart(out.getPos());
  }

  public void startData() throws IOException {
    currentColumn.setDataStart(out.getPos());
    state = state.startData();
  }

  public void endColumn() throws IOException {
    state = state.endColumn();
    currentColumn.setDataEnd(out.getPos());
    currentBlock.addColumn(currentColumn);
    if (INFO) LOG.info(currentColumn);
    currentColumn = null;
  }

  public void endBlock() throws IOException {
    state = state.endBlock();
    currentBlock.setEndIndex(out.getPos());
    currentBlock.setRecordCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
  }

  public void end() throws IOException {
    state = state.end();
    long footerIndex = out.getPos();
    out.writeInt(CURRENT_VERSION);
    Footer footer = new Footer(schema.toString(), blocks);
//    out.writeUTF(Footer.toJSON(footer));
    // lazy: use serialization
    // TODO: change that
    new ObjectOutputStream(out).writeObject(footer);
    out.writeLong(footerIndex);
    out.write(MAGIC);
    out.close();
  }

}
