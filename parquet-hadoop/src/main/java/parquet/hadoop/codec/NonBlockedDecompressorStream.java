package parquet.hadoop.codec;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;

/**
 * DecompressorStream class that should be used instead of the default hadoop DecompressorStream
 * object. Hadoop's compressor adds blocking ontop of the compression codec. We don't want
 * that since our Pages already solve the need to add blocking.
 */
public class NonBlockedDecompressorStream extends DecompressorStream {
  private boolean inputHandled;
  
  public NonBlockedDecompressorStream(InputStream stream, Decompressor decompressor, int bufferSize) throws IOException {
	super(stream, decompressor, bufferSize);
  }
  
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
	if (!inputHandled) {
	  // Send all the compressed input to the decompressor.
	  while (true) {
		int compressedBytes = getCompressedData();
		if (compressedBytes == -1) break;
		decompressor.setInput(buffer, 0, compressedBytes);
	  }
	  inputHandled = true;
	}
	
	int decompressedBytes = decompressor.decompress(b, off, len);
	if (decompressor.finished()) {
	  decompressor.reset();
	}
	return decompressedBytes;
  }
}
