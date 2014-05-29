package parquet.hadoop.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
	
  protected ByteBuffer byteBuf;
  protected int initPos;
  protected int count;
  public ByteBufferInputStream(ByteBuffer buffer) {
    this(buffer, buffer.position(), buffer.limit());
  }
  
  public ByteBufferInputStream(ByteBuffer buffer, int offset, int count) {
    byteBuf = buffer;
    this.initPos = offset;
    this.count = count;
  }
  
  @Override
  public int read() throws IOException {
    if (byteBuf.position() - initPos > count) {
    	return -1;
    }
    return  byteBuf.get();
  }
}
