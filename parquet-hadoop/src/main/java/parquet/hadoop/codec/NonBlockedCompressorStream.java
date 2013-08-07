package parquet.hadoop.codec;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;

/**
 * CompressorStream class that should be used instead of the default hadoop CompressorStream
 * object. Hadoop's compressor adds blocking ontop of the compression codec. We don't want
 * that since our Pages already solve the need to add blocking.
 */
public class NonBlockedCompressorStream extends CompressorStream {
  public NonBlockedCompressorStream(OutputStream stream, Compressor compressor, int bufferSize) {
	super(stream, compressor, bufferSize);
  }
  
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // Sanity checks
    if (compressor.finished()) {
      throw new IOException("write beyond end of stream");
    }
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }
    compressor.setInput(b, off, len);    
  }
}
