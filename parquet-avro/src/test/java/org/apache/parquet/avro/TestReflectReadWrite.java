/**
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
package org.apache.parquet.avro;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Stringable;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestReflectReadWrite {

  @Test
  public void testReadWriteReflect() throws IOException {
    Configuration conf = new Configuration(false);
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);

    Path path = writePojosToParquetFile(10, CompressionCodecName.UNCOMPRESSED, false);
    try (ParquetReader<Pojo> reader = new AvroParquetReader<Pojo>(conf, path)) {
      Pojo object = getPojo();
      for (int i = 0; i < 10; i++) {
        assertEquals(object, reader.read());
      }
      assertNull(reader.read());
    }
  }

  @Test
  public void testWriteReflectReadGeneric() throws IOException {
    Configuration conf = new Configuration(false);
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    AvroReadSupport.setAvroDataSupplier(conf, GenericDataSupplier.class);

    Path path = writePojosToParquetFile(2, CompressionCodecName.UNCOMPRESSED, false);
    try (ParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(conf, path)) {
      GenericRecord object = getGenericPojoUtf8();
      for (int i = 0; i < 2; i += 1) {
        assertEquals(object, reader.read());
      }
      assertNull(reader.read());
    }
  }

  private GenericRecord getGenericPojoUtf8() {
    Schema schema = ReflectData.get().getSchema(Pojo.class);
    GenericData.Record record = new GenericData.Record(schema);
    record.put("myboolean", true);
    record.put("mybyte", 1);
    record.put("myshort", 1);
    record.put("myint", 1);
    record.put("mylong", 2L);
    record.put("myfloat", 3.1f);
    record.put("mydouble", 4.1);
    record.put("mybytes", ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }));
    record.put("mystring", new Utf8("Hello"));
    record.put("myenum", new GenericData.EnumSymbol(schema.getField("myenum").schema(), "A"));
    Map<CharSequence, CharSequence> map = new HashMap<CharSequence, CharSequence>();
    map.put(new Utf8("a"), new Utf8("1"));
    map.put(new Utf8("b"), new Utf8("2"));
    record.put("mymap", map);
    record.put("myshortarray",
        new GenericData.Array<Integer>(schema.getField("myshortarray").schema(), Lists.newArrayList(1, 2)));
    record.put("myintarray",
        new GenericData.Array<Integer>(schema.getField("myintarray").schema(), Lists.newArrayList(1, 2)));
    record.put("mystringarray", new GenericData.Array<Utf8>(schema.getField("mystringarray").schema(),
        Lists.newArrayList(new Utf8("a"), new Utf8("b"))));
    record.put("mylist", new GenericData.Array<Utf8>(schema.getField("mylist").schema(),
        Lists.newArrayList(new Utf8("a"), new Utf8("b"), new Utf8("c"))));
    record.put("mystringable", new StringableObj("blah blah"));
    return record;
  }

  private Pojo getPojo() {
    Pojo object = new Pojo();
    object.myboolean = true;
    object.mybyte = 1;
    object.myshort = 1;
    object.myint = 1;
    object.mylong = 2L;
    object.myfloat = 3.1f;
    object.mydouble = 4.1;
    object.mybytes = new byte[] { 1, 2, 3, 4 };
    object.mystring = "Hello";
    object.myenum = E.A;
    Map<String, String> map = new HashMap<String, String>();
    map.put("a", "1");
    map.put("b", "2");
    object.mymap = map;
    object.myshortarray = new short[] { 1, 2 };
    object.myintarray = new int[] { 1, 2 };
    object.mystringarray = new String[] { "a", "b" };
    object.mylist = Lists.newArrayList("a", "b", "c");
    object.mystringable = new StringableObj("blah blah");
    return object;
  }

  private Path writePojosToParquetFile(int num, CompressionCodecName compression, boolean enableDictionary)
      throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    Pojo object = getPojo();

    Schema schema = ReflectData.get().getSchema(object.getClass());
    try (ParquetWriter<Pojo> writer = AvroParquetWriter.<Pojo>builder(path).withSchema(schema)
        .withCompressionCodec(compression).withDataModel(ReflectData.get()).withDictionaryEncoding(enableDictionary)
        .build()) {
      for (int i = 0; i < num; i++) {
        writer.write(object);
      }
    }
    return path;
  }

  public static enum E {
    A, B
  }

  public static class StringableObj {
    private String value;

    public StringableObj(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof StringableObj && this.value.equals(((StringableObj) other).value);
    }
  }

  public static class Pojo {
    public boolean myboolean;
    public byte mybyte;
    public short myshort;
    // no char until https://issues.apache.org/jira/browse/AVRO-1458 is fixed
    public int myint;
    public long mylong;
    public float myfloat;
    public double mydouble;
    public byte[] mybytes;
    public String mystring;
    public E myenum;
    private Map<String, String> mymap;
    private short[] myshortarray;
    private int[] myintarray;
    private String[] mystringarray;
    private List<String> mylist;
    @Stringable
    private StringableObj mystringable;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Pojo))
        return false;
      Pojo that = (Pojo) o;
      return myboolean == that.myboolean && mybyte == that.mybyte && myshort == that.myshort && myint == that.myint
          && mylong == that.mylong && myfloat == that.myfloat && mydouble == that.mydouble
          && Arrays.equals(mybytes, that.mybytes) && mystring.equals(that.mystring) && myenum == that.myenum
          && mymap.equals(that.mymap) && Arrays.equals(myshortarray, that.myshortarray)
          && Arrays.equals(myintarray, that.myintarray) && Arrays.equals(mystringarray, that.mystringarray)
          && mylist.equals(that.mylist) && mystringable.equals(that.mystringable);
    }

    @Override
    public String toString() {
      return "Pojo{" + "myboolean=" + myboolean + ", mybyte=" + mybyte + ", myshort=" + myshort + ", myint=" + myint
          + ", mylong=" + mylong + ", myfloat=" + myfloat + ", mydouble=" + mydouble + ", mybytes="
          + Arrays.toString(mybytes) + ", mystring='" + mystring + '\'' + ", myenum=" + myenum + ", mymap=" + mymap
          + ", myshortarray=" + Arrays.toString(myshortarray) + ", myintarray=" + Arrays.toString(myintarray)
          + ", mystringarray=" + Arrays.toString(mystringarray) + ", mylist=" + mylist + ", mystringable="
          + mystringable.toString() + '}';
    }
  }

}
