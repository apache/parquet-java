/**
 * Copyright 2013 Criteo.
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
package parquet.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import parquet.hive.serde.ParquetHiveSerDe;
import parquet.hive.writable.BinaryWritable;

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
      final MapWritable map = new MapWritable();

      map.put(new Text("abyte"), new ByteWritable((byte) 123));
      map.put(new Text("ashort"), new ShortWritable((short) 456));
      map.put(new Text("aint"), new IntWritable(789));
      map.put(new Text("along"), new LongWritable(1000l));
      map.put(new Text("adouble"), new DoubleWritable((double) 5.3));
      map.put(new Text("astring"), new BinaryWritable("hive and hadoop and parquet. Big family."));

      // Test
      deserializeAndSerializeLazySimple(serDe, map);
      System.out.println("test: testParquetHiveSerDe - OK");

    } catch (final Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void deserializeAndSerializeLazySimple(final ParquetHiveSerDe serDe, final MapWritable t)
      throws SerDeException {

    // Get the row structure
    final StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();

    // Deserialize
    final Object row = serDe.deserialize(t);
    assertEquals("deserialize gave the wrong object", row.getClass(), MapWritable.class);
    assertEquals("serialized size correct after deserialization", serDe.getSerDeStats()
        .getRawDataSize(), t.size());
    assertEquals("deserialisation give the wrong object", t, row);

    // Serialize
    final MapWritable serializedMap = (MapWritable) serDe.serialize(row, oi);
    assertEquals("serialized size correct after serialization", serDe.getSerDeStats()
        .getRawDataSize(),
        serializedMap.size());
    assertTrue("serialize Object to MapWritable should be equals", mapEquals(t, serializedMap));
  }


  private boolean mapEquals(final MapWritable first, final MapWritable second) {

    if (first == second) {
      return true;
    }

    if (second instanceof MapWritable) {
      if (first.size() != second.size()) {
        return false;
      }
      return first.entrySet().equals(second.entrySet());
    }
    return false;
  }

  private Properties createProperties() {
    final Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty("columns",
        "abyte,ashort,aint,along,adouble,astring");
    tbl.setProperty("columns.types",
        "tinyint:smallint:int:bigint:double:string");
    tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }

}
