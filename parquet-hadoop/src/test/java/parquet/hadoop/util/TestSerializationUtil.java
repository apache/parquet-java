package parquet.hadoop.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Serialization utils copied from:
 * https://github.com/kevinweil/elephant-bird/blob/master/core/src/test/java/com/twitter/elephantbird/util/TestHadoopUtils.java
 *
 * TODO: Refactor elephant-bird so that we can depend on utils like this without extra baggage.
 */
public class TestSerializationUtil {

  @Test
  public void testReadWriteObjectToConfAsBase64() throws Exception {
    Map<Integer, String> anObject = new HashMap<Integer, String>();
    anObject.put(7, "seven");
    anObject.put(8, "eight");

    Configuration conf = new Configuration();

    SerializationUtil.writeObjectToConfAsBase64("anobject", anObject, conf);
    Map<Integer, String> copy = SerializationUtil.readObjectFromConfAsBase64("anobject", conf);
    assertEquals(anObject, copy);

    try {
      Set<String> bad = SerializationUtil.readObjectFromConfAsBase64("anobject", conf);
      fail("This should throw a ClassCastException");
    } catch (ClassCastException e) {

    }

    conf = new Configuration();
    Object nullObj = null;

    SerializationUtil.writeObjectToConfAsBase64("anobject", null, conf);
    Object copyObj = SerializationUtil.readObjectFromConfAsBase64("anobject", conf);
    assertEquals(nullObj, copyObj);
  }

  @Test
  public void readObjectFromConfAsBase64UnsetKey() throws Exception {
    assertNull(SerializationUtil.readObjectFromConfAsBase64("non-existant-key", new Configuration()));
  }
}