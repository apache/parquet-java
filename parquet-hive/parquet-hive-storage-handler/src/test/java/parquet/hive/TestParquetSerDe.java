/**
 * Copyright 2013 Criteo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License
 * at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package parquet.hive;

import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import parquet.hive.serde.ParquetHiveSerDe;
import parquet.hive.writable.BinaryWritable;
import parquet.io.api.Binary;

/**
 *
 * testParquetHiveSerDe
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class TestParquetSerDe {

  @Test
  public void testParquetHiveSerDe() throws Throwable {
    try {
      // Create the SerDe
      System.out.println("test: testParquetHiveSerDe");

      final ParquetHiveSerDe serDe = new ParquetHiveSerDe();
      final Configuration conf = new Configuration();
      final Properties tbl = createProperties();
      serDe.initialize(conf, tbl);

      // Data
      final Writable[] arr = new Writable[8];

      arr[0] = new ByteWritable((byte) 123);
      arr[1] = new ShortWritable((short) 456);
      arr[2] = new IntWritable(789);
      arr[3] = new LongWritable(1000l);
      arr[4] = new DoubleWritable((double) 5.3);
      arr[5] = new BinaryWritable(Binary.fromString("hive and hadoop and parquet. Big family."));

      final Writable[] mapContainer = new Writable[1];
      final Writable[] map = new Writable[3];
      for (int i = 0; i < 3; ++i) {
        final Writable[] pair = new Writable[2];
        pair[0] = new BinaryWritable(Binary.fromString("key_" + i));
        pair[1] = new IntWritable(i);
        map[i] = new ArrayWritable(Writable.class, pair);
      }
      mapContainer[0] = new ArrayWritable(Writable.class, map);
      arr[6] = new ArrayWritable(Writable.class, mapContainer);

      final Writable[] arrayContainer = new Writable[1];
      final Writable[] array = new Writable[5];
      for (int i = 0; i < 5; ++i) {
        array[i] = new BinaryWritable(Binary.fromString("elem_" + i));
      }
      arrayContainer[0] = new ArrayWritable(Writable.class, array);
      arr[7] = new ArrayWritable(Writable.class, arrayContainer);

      final ArrayWritable arrWritable = new ArrayWritable(Writable.class, arr);
      // Test
      deserializeAndSerializeLazySimple(serDe, arrWritable);
      System.out.println("test: testParquetHiveSerDe - OK");

    } catch (final Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void deserializeAndSerializeLazySimple(final ParquetHiveSerDe serDe, final ArrayWritable t) throws SerDeException {

    // Get the row structure
    final StructObjectInspector oi = (StructObjectInspector) serDe.getObjectInspector();

    // Deserialize
    final Object row = serDe.deserialize(t);
    assertEquals("deserialization gives the wrong object class", row.getClass(), ArrayWritable.class);
    assertEquals("size correct after deserialization", serDe.getSerDeStats().getRawDataSize(), t.get().length);
    assertEquals("deserialization gives the wrong object", t, row);

    // Serialize
    final ArrayWritable serializedArr = (ArrayWritable) serDe.serialize(row, oi);
    assertEquals("size correct after serialization", serDe.getSerDeStats().getRawDataSize(), serializedArr.get().length);
    assertTrue("serialized object should be equal to starting object", UtilitiesTestMethods.arrayWritableEquals(t, serializedArr));
  }

  private Properties createProperties() {
    final Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty("columns", "abyte,ashort,aint,along,adouble,astring,amap,alist");
    tbl.setProperty("columns.types", "tinyint:smallint:int:bigint:double:string:map<string,int>:array<string>");
    tbl.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }
}
