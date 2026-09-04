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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Serialization utils copied from:
 * https://github.com/kevinweil/elephant-bird/blob/master/core/src/test/java/com/twitter/elephantbird/util/TestHadoopUtils.java
 * <p>
 * TODO: Refactor elephant-bird so that we can depend on utils like this without extra baggage.
 */
public class TestSerializationUtil {

  @Test
  public void testReadWriteObjectToConfAsBase64() throws Exception {
    Map<Integer, String> anObject = new HashMap<>();
    anObject.put(7, "seven");
    anObject.put(8, "eight");

    Configuration confWithObject = new Configuration();

    SerializationUtil.writeObjectToConfAsBase64("anobject", anObject, confWithObject);
    Map<Integer, String> copy = SerializationUtil.readObjectFromConfAsBase64("anobject", confWithObject);
    assertThat(copy).isEqualTo(anObject);

    assertThatThrownBy(() -> {
          Set<String> bad = SerializationUtil.readObjectFromConfAsBase64("anobject", confWithObject);
        })
        .isInstanceOf(ClassCastException.class)
        .hasMessageContaining("cannot be cast to class java.util.Set");

    Configuration conf = new Configuration();
    Object nullObj = null;

    SerializationUtil.writeObjectToConfAsBase64("anobject", null, conf);
    Object copyObj = SerializationUtil.readObjectFromConfAsBase64("anobject", conf);
    assertThat(copyObj).isEqualTo(nullObj);
  }

  @Test
  public void readObjectFromConfAsBase64UnsetKey() throws Exception {
    assertThat(SerializationUtil.<Object>readObjectFromConfAsBase64("non-existant-key", new Configuration()))
        .isNull();
  }
}
