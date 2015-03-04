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
package parquet.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import parquet.Closeables;
import parquet.Log;

/**
 * Serialization utils copied from:
 * https://github.com/kevinweil/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/util/HadoopUtils.java
 *
 * TODO: Refactor elephant-bird so that we can depend on utils like this without extra baggage.
 */
public final class SerializationUtil {
  private static final Log LOG = Log.getLog(SerializationUtil.class);

  private SerializationUtil() { }

  /**
   * Reads an object (that was written using
   * {@link #writeObjectToConfAsBase64}) from a configuration.
   *
   * @param key for the configuration
   * @param conf to read from
   * @return the read object, or null if key is not present in conf
   * @throws IOException
   */
  public static void writeObjectToConfAsBase64(String key, Object obj, Configuration conf) throws IOException {
    ByteArrayOutputStream baos = null;
    GZIPOutputStream gos = null;
    ObjectOutputStream oos = null;

    try {
      baos = new ByteArrayOutputStream();
      gos = new GZIPOutputStream(baos);
      oos = new ObjectOutputStream(gos);
      oos.writeObject(obj);
    } finally {
      Closeables.close(oos);
      Closeables.close(gos);
      Closeables.close(baos);
    }

    conf.set(key, new String(Base64.encodeBase64(baos.toByteArray()), "UTF-8"));
  }

  /**
   * Reads an object (that was written using
   * {@link #writeObjectToConfAsBase64}) from a configuration
   *
   * @param key for the configuration
   * @param conf to read from
   * @return the read object, or null if key is not present in conf
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <T> T readObjectFromConfAsBase64(String key, Configuration conf) throws IOException {
    String b64 = conf.get(key);
    if (b64 == null) {
      return null;
    }

    byte[] bytes = Base64.decodeBase64(b64.getBytes("UTF-8"));

    ByteArrayInputStream bais = null;
    GZIPInputStream gis = null;
    ObjectInputStream ois = null;

    try {
      bais = new ByteArrayInputStream(bytes);
      gis = new GZIPInputStream(bais);
      ois = new ObjectInputStream(gis);
      return (T) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not read object from config with key " + key, e);
    } catch (ClassCastException e) {
      throw new IOException("Couldn't cast object read from config with key " + key, e);
    } finally {
      Closeables.close(ois);
      Closeables.close(gis);
      Closeables.close(bais);
    }
  }
}
