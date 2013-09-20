/**
 * Copyright 2012 Twitter, Inc.
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
package parquet.avro;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static parquet.filter.ColumnRecordFilter.column;
import static parquet.filter.ColumnPredicates.equalTo;
import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

/**
 * Other tests exercise the use of Avro Generic, a dynamic data representation. This class focuses
 * on Avro Speific whose schemas are pre-compiled to POJOs with built in SerDe for faster serialization.
 */
public class TestSpecificReadWrite {

  @Test
  public void testReadWriteSpecific() throws IOException {
    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, false);
    ParquetReader<Car> reader = new AvroParquetReader<Car>(path);
    for (int i = 0; i < 10; i++) {
      assertEquals(getVwPolo().toString(),reader.read().toString());
      assertEquals(getVwPassat().toString(),reader.read().toString());
      assertEquals(getBmwMini().toString(),reader.read().toString());
    }
    assertNull(reader.read());
  }

  @Test
  public void testReadWriteSpecificWithDictionary() throws IOException {
    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, true);
    ParquetReader<Car> reader = new AvroParquetReader<Car>(path);
    for (int i = 0; i < 10; i++) {
      assertEquals(getVwPolo().toString(),reader.read().toString());
      assertEquals(getVwPassat().toString(),reader.read().toString());
      assertEquals(getBmwMini().toString(),reader.read().toString());
    }
    assertNull(reader.read());
  }

  @Test
  public void testFilterMatchesMultiple() throws IOException {

    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, false);

    ParquetReader<Car> reader = new AvroParquetReader<Car>(path, column("make", equalTo("Volkswagen")));
    for (int i = 0; i < 10; i++) {
      assertEquals(getVwPolo().toString(),reader.read().toString());
      assertEquals(getVwPassat().toString(),reader.read().toString());
    }
    assertNull( reader.read());
  }

  @Test
  public void testFilterWithDictionary() throws IOException {

    Path path = writeCarsToParquetFile(1,CompressionCodecName.UNCOMPRESSED,true);

    ParquetReader<Car> reader = new AvroParquetReader<Car>(path, column("make", equalTo("Volkswagen")));
    assertEquals(getVwPolo().toString(),reader.read().toString());
    assertEquals(getVwPassat().toString(),reader.read().toString());
    assertNull( reader.read());
  }

  @Test
  public void testFilterOnSubAttribute() throws IOException {

    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, false);

    ParquetReader<Car> reader = new AvroParquetReader<Car>(path, column("engine.type", equalTo(EngineType.DIESEL)));
    assertEquals(reader.read().toString(),getVwPassat().toString());
    assertNull( reader.read());

    reader = new AvroParquetReader<Car>(path, column("engine.capacity", equalTo(1.4f)));
    assertEquals(getVwPolo().toString(),reader.read().toString());
    assertNull( reader.read());


    reader = new AvroParquetReader<Car>(path, column("engine.hasTurboCharger", equalTo(true)));
    assertEquals(getBmwMini().toString(),reader.read().toString());
    assertNull( reader.read());
  }

  private Path writeCarsToParquetFile( int num, CompressionCodecName compression, boolean enableDictionary) throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    Car vwPolo   = getVwPolo();
    Car vwPassat = getVwPassat();
    Car bmwMini  = getBmwMini();

    ParquetWriter<Car> writer = new AvroParquetWriter<Car>(path,Car.SCHEMA$, compression,
        DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary);
    for (int i = 0; i < num; i++) {
      writer.write(vwPolo);
      writer.write(vwPassat);
      writer.write(bmwMini);
    }
    writer.close();
    return path;
  }

  public static Car getVwPolo() {
    String vin = "WVWDB4505LK000000";
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A123 GTR")
        .setMake("Volkswagen")
        .setModel("Polo")
        .setVin(new Vin(vin.getBytes()))
        .setDoors(4)
        .setEngine(Engine.newBuilder().setType(EngineType.PETROL)
                  .setCapacity(1.4f).setHasTurboCharger(false).build())
        .setOptionalExtra(
            Stereo.newBuilder().setMake("Blaupunkt").setSpeakers(4).build())
        .setServiceHistory(ImmutableList.of(
            Service.newBuilder().setDate(1325376000l).setMechanic("Jim").build(),
            Service.newBuilder().setDate(1356998400l).setMechanic("Mike").build()))
        .build();
  }

  public static Car getVwPassat() {
    String vin = "WVWDB4505LK000001";
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A123 GXR")
        .setMake("Volkswagen")
        .setModel("Passat")
        .setVin(new Vin(vin.getBytes()))
        .setDoors(5)
        .setEngine(Engine.newBuilder().setType(EngineType.DIESEL)
            .setCapacity(2.0f).setHasTurboCharger(false).build())
        .setOptionalExtra(
            LeatherTrim.newBuilder().setColour("Black").build())
        .setServiceHistory(ImmutableList.of(
            Service.newBuilder().setDate(1325376000l).setMechanic("Jim").build()))
        .build();
  }

  public static Car getBmwMini() {
    String vin = "WBABA91060AL00000";
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A124 GSR")
        .setMake("BMW")
        .setModel("Mini")
        .setVin(new Vin(vin.getBytes()))
        .setDoors(4)
        .setEngine(Engine.newBuilder().setType(EngineType.PETROL)
            .setCapacity(1.6f).setHasTurboCharger(true).build())
        .setOptionalExtra(null)
        .setServiceHistory(ImmutableList.of(
            Service.newBuilder().setDate(1356998400l).setMechanic("Mike").build()))
        .build();
  }
}
