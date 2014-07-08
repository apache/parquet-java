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
 * TODO(alexlevenson): Refactor elephant-bird so that we can depend on utils like this without extra baggage.
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
      LOG.error("Could not read object from config with key " + key, e);
      throw new IOException(e);
    } catch (ClassCastException e) {
      LOG.error("Couldn't cast object read from config with key " + key, e);
      throw new IOException(e);
    } finally {
      Closeables.close(ois);
      Closeables.close(gis);
      Closeables.close(bais);
    }
  }
}
