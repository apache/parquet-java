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