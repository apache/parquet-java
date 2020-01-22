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
package org.apache.parquet.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;

/**
 * Serialization utils copied from:
 * https://github.com/kevinweil/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/util/HadoopUtils.java
 *
 * TODO: Refactor elephant-bird so that we can depend on utils like this without
 * extra baggage.
 */
public final class SerializationUtil {

  private SerializationUtil() {
  }

  /**
   * Writes an object to a configuration.
   *
   * @param key for the configuration
   * @param obj the object to write
   * @param conf to read from
   * @throws IOException if there is an error while writing
   */
  public static void writeObjectToConfAsBase64(String key, Object obj, Configuration conf) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (GZIPOutputStream gos = new GZIPOutputStream(baos); ObjectOutputStream oos = new ObjectOutputStream(gos)) {
        oos.writeObject(obj);
      }
      conf.set(key, new String(Base64.getMimeEncoder().encode(baos.toByteArray()), StandardCharsets.UTF_8));
    }
  }

  /**
   * Reads an object (that was written using {@link #writeObjectToConfAsBase64})
   * from a configuration
   *
   * @param key for the configuration
   * @param conf to read from
   * @param <T> the Java type of the deserialized object
   * @return the read object, or null if key is not present in conf
   * @throws IOException if there is an error while reading
   */
  @SuppressWarnings("unchecked")
  public static <T> T readObjectFromConfAsBase64(String key, Configuration conf) throws IOException {
    String b64 = conf.get(key);
    if (b64 == null) {
      return null;
    }

    byte[] bytes = Base64.getMimeDecoder().decode(b64.getBytes(StandardCharsets.UTF_8));

    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        GZIPInputStream gis = new GZIPInputStream(bais);
        ObjectInputStream ois = new ObjectInputStream(gis)) {
      return (T) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not read object from config with key " + key, e);
    } catch (ClassCastException e) {
      throw new IOException("Could not cast object read from config with key " + key, e);
    }
  }
}
