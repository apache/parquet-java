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
