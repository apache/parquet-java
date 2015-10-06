/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressionCodec;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.Preconditions;

public class DirectCodecPool {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectCodecPool.class);

  public static final DirectCodecPool INSTANCE = new DirectCodecPool();

  @SuppressWarnings("unchecked")
  private final Map<CompressionCodec, CodecPool> codecs =
      (Map<CompressionCodec, CodecPool>) (Object) Collections.synchronizedMap(new HashMap<Object, Object>());

  @SuppressWarnings("unchecked")
  private final Map<Class<?>, GenericObjectPool> directDePools = (Map<Class<?>, GenericObjectPool>) (Object) Collections
      .synchronizedMap(new HashMap<Object, Object>());
  private final Map<Class<?>, GenericObjectPool> dePools = (Map<Class<?>, GenericObjectPool>) (Object) Collections
      .synchronizedMap(new HashMap<Object, Object>());
  private final Map<Class<?>, GenericObjectPool> cPools = (Map<Class<?>, GenericObjectPool>) (Object) Collections
      .synchronizedMap(new HashMap<Object, Object>());

  private DirectCodecPool() {
  }

  public class CodecPool {
    private final GenericObjectPool compressorPool;
    private final GenericObjectPool decompressorPool;
    private final GenericObjectPool directDecompressorPool;
    private final boolean supportDirectDecompressor;

    private CodecPool(final CompressionCodec codec){
      try {
        boolean supportDirectDecompressor = codec instanceof DirectDecompressionCodec;
        compressorPool = new GenericObjectPool(new BasePoolableObjectFactory() {
          public Object makeObject() throws Exception {
            return codec.createCompressor();
          }
        }, Integer.MAX_VALUE);

        Object com = compressorPool.borrowObject();
        if (com != null) {
          cPools.put(com.getClass(), compressorPool);
          compressorPool.returnObject(com);
        }else{
          logger.warn("Unable to find compressor for codec {}", codec.getClass().getName());
        }

        decompressorPool = new GenericObjectPool(new BasePoolableObjectFactory() {
          public Object makeObject() throws Exception {
            return codec.createDecompressor();
          }
        }, Integer.MAX_VALUE);

        Object decom = decompressorPool.borrowObject();
        if (decom != null) {
          dePools.put(decom.getClass(), decompressorPool);
          decompressorPool.returnObject(decom);
        } else {
          logger.warn("Unable to find decompressor for codec {}", codec.getClass().getName());
        }

        if (supportDirectDecompressor) {
          directDecompressorPool = new GenericObjectPool(new BasePoolableObjectFactory() {
            public Object makeObject() throws Exception {
              return ((DirectDecompressionCodec) codec).createDirectDecompressor();
            }
          }, Integer.MAX_VALUE);

          Object ddecom = directDecompressorPool.borrowObject();
          if (ddecom != null) {
            directDePools.put(ddecom.getClass(), directDecompressorPool);
            directDecompressorPool.returnObject(ddecom);

          } else {
            supportDirectDecompressor = false;
            logger.warn("Unable to find direct decompressor for codec {}", codec.getClass().getName());
          }

        } else {
          directDecompressorPool = null;
        }

        this.supportDirectDecompressor = supportDirectDecompressor;
      } catch (Exception e) {
        throw new ParquetRuntimeException("Error creating compression codec pool.") {
          // This exception will not likely be recoverable
        };
      }
    }

    public DirectDecompressor borrowDirectDecompressor(){
      Preconditions.checkArgument(supportDirectDecompressor, "Tried to get a direct Decompressor from a non-direct codec.");
      try {
        return (DirectDecompressor) directDecompressorPool.borrowObject();
      } catch (Exception e) {
        throw new ParquetCompressionCodecException(e);
      }
    }

    public boolean supportsDirectDecompression() {
      return supportDirectDecompressor;
    }

    public Decompressor borrowDecompressor(){
      try {
        return (Decompressor) decompressorPool.borrowObject();
      } catch (Exception e) {
        throw new ParquetCompressionCodecException(e);
      }
    }

    public Compressor borrowCompressor(){
      try {
        return (Compressor) compressorPool.borrowObject();
      } catch (Exception e) {
        throw new ParquetCompressionCodecException(e);
      }
    }
  }

  public CodecPool codec(CompressionCodec codec){
    CodecPool pools = codecs.get(codec);
    if(pools == null){
      synchronized(this){
        pools = codecs.get(codec);
        if(pools == null){
          pools = new CodecPool(codec);
          codecs.put(codec, pools);
        }
      }
    }
    return pools;
  }

  private void returnToPool(Object obj, Map<Class<?>, GenericObjectPool> pools) {
    try {
      GenericObjectPool pool = pools.get(obj.getClass());
      if (pool == null) {
        throw new IllegalStateException("Received unexpected decompressor.");
      }
      pool.returnObject(obj);
    } catch (Exception e) {
      throw new ParquetCompressionCodecException(e);
    }

  }

  public void returnCompressor(Compressor compressor) {
    returnToPool(compressor, cPools);
  }

  public void returnDecompressor(Decompressor decompressor) {
    returnToPool(decompressor, dePools);
  }

  public void returnDecompressor(DirectDecompressor decompressor) {
    returnToPool(decompressor, directDePools);
  }

  public static class ParquetCompressionCodecException extends ParquetRuntimeException {

    public ParquetCompressionCodecException() {
      super();
    }

    public ParquetCompressionCodecException(String message, Throwable cause) {
      super(message, cause);
    }

    public ParquetCompressionCodecException(String message) {
      super(message);
    }

    public ParquetCompressionCodecException(Throwable cause) {

    }
  }
}

