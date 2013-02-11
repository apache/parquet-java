/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redelm.hadoop;

import java.util.HashMap;
import java.util.Map;

import redelm.hadoop.metadata.CompressionCodecName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class CodecFactory {
  private final Map<String, CompressionCodec> codecByName = new HashMap<String, CompressionCodec>();
  private final Configuration configuration;

  public CodecFactory(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   *
   * @param codecName the requested codec
   * @return the corresponding hadoop codec. null if UNCOMPRESSED
   */
  public CompressionCodec getCodec(CompressionCodecName codecName) {
    String codecClassName = codecName.getHadoopCompressionCodecClass();
    if (codecClassName == null) {
      return null;
    } else if (codecByName.containsKey(codecClassName)) {
      return codecByName.get(codecClassName);
    } else {
      try {
        Class<?> codecClass = Class.forName(codecClassName);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, configuration);
        codecByName.put(codecClassName, codec);
        return codec;
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Class " + codecClassName + " was not found", e);
      }
    }
  }
}
