package parquet.bytes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
	
  protected ByteBuffer byteBuf;
  protected int initPos;
  protected int count;
  public ByteBufferInputStream(ByteBuffer buffer) {
    this(buffer, buffer.position(), buffer.remaining());
  }
  
  public ByteBufferInputStream(ByteBuffer buffer, int offset, int count) {
    buffer.position(offset);
    byteBuf = buffer.slice();
    byteBuf.limit(count);
    this.initPos = offset;
    this.count = count;
  }
  
  public ByteBuffer toByteBuffer() {
    return byteBuf.slice();
  }
  
  @Override
  public int read() throws IOException {
    if (!byteBuf.hasRemaining()) {
    	return -1;
    }
    //Workaround for unsigned byte
    return byteBuf.get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    int count = Math.min(byteBuf.remaining(), length);
    if (count == 0) return -1;
    byteBuf.get(bytes, offset, count);
    return count;
  }
  
  @Override
  public long skip(long n) {
	  if (n > byteBuf.remaining())
	    n = byteBuf.remaining();
	  int pos = byteBuf.position();
	  byteBuf.position((int)(pos + n));
	  return n;
  }


  @Override
  public int available() {
    return byteBuf.remaining();
  }
}
