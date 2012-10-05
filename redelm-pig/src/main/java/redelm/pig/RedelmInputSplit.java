package redelm.pig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class RedelmInputSplit extends InputSplit implements Serializable, Writable {
  private static final long serialVersionUID = 1L;

  private String path;
  private long start;
  private long length;
  private String[] hosts;
  private BlockMetaData block;
  private String schemaString;

  public RedelmInputSplit() {
  }

  public RedelmInputSplit(Path path, long start, long length, String[] hosts, BlockMetaData block, String schemaString) {
    System.out.println("RedelmInputSplit("+path+", "+start+", "+length+", "+Arrays.toString(hosts)+", "+block+", "+schemaString+")");
    this.path = path.toUri().toString();
    this.start = start;
    this.length = length;
    this.hosts = hosts;
    this.block = block;
    this.schemaString = schemaString;
  }

  public BlockMetaData getBlock() {
    return block;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return hosts;
  }

  public long getStart() {
    return start;
  }

  public Path getPath() {
    try {
      return new Path(new URI(path));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int l = in.readInt();
    byte[] b = new byte[l];
    in.readFully(b);
    RedelmInputSplit other;
    try {
      other = (RedelmInputSplit)
          new ObjectInputStream(new ByteArrayInputStream(b))
        .readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("wrong class serialized", e);
    }
    this.path = other.path;
    this.start = other.start;
    this.length = other.length;
    this.hosts = other.hosts;
    this.block = other.block;
    this.schemaString = other.schemaString;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new ObjectOutputStream(baos).writeObject(this);
    byte[] b = baos.toByteArray();
    out.writeInt(b.length);
    out.write(b);
  }

  public String getSchema() {
    return schemaString;
  }

}
