/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
