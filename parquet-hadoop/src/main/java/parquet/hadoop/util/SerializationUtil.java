package parquet.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import parquet.Log;

/**
 * Serialization utils copied from:
 * https://github.com/kevinweil/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/util/HadoopUtils.java
 *
 * TODO: Should we depend on elephant-bird or just copy here?
 */
public final class SerializationUtil {
  private static final Log LOG = Log.getLog(SerializationUtil.class);

  private SerializationUtil() { }

  /**
   * Writes an object into a configuration by converting it to a base64 encoded string
   * obj must be Serializable
   *
   * @param key for the configuration
   * @param obj to write
   * @param conf to write to
   * @throws IOException
   */
  public static void writeObjectToConfAsBase64(String key, Object obj, Configuration conf) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
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
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    try {
      return (T) ois.readObject();
    } catch (ClassNotFoundException e) {
      LOG.error("Could not read object from config with key " + key, e);
      throw new IOException(e);
    } catch (ClassCastException e) {
      LOG.error("Couldn't cast object read from config with key " + key, e);
      throw new IOException(e);
    }
  }
}
