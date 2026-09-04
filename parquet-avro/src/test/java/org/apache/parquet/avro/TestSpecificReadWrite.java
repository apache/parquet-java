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
package org.apache.parquet.avro;

import static org.apache.parquet.filter.ColumnPredicates.equalTo;
import static org.apache.parquet.filter.ColumnRecordFilter.column;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Other tests exercise the use of Avro Generic, a dynamic data representation. This class focuses
 * on Avro Speific whose schemas are pre-compiled to POJOs with built in SerDe for faster serialization.
 */
public class TestSpecificReadWrite {

  private static Configuration testConf(boolean compat) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, compat);
    return conf;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCompatReadWriteSpecific(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, false);
    try (ParquetReader<Car> reader = new AvroParquetReader<>(testConf, path)) {
      for (int i = 0; i < 10; i++) {
        assertThat(reader.read()).asString().isEqualTo(getVwPolo().toString());
        assertThat(reader.read()).asString().isEqualTo(getVwPassat().toString());
        assertThat(reader.read()).asString().isEqualTo(getBmwMini().toString());
      }
      assertThat(reader.read()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReadWriteSpecificWithDictionary(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, true);
    try (ParquetReader<Car> reader = new AvroParquetReader<>(testConf, path)) {
      for (int i = 0; i < 10; i++) {
        assertThat(reader.read()).asString().isEqualTo(getVwPolo().toString());
        assertThat(reader.read()).asString().isEqualTo(getVwPassat().toString());
        assertThat(reader.read()).asString().isEqualTo(getBmwMini().toString());
      }
      assertThat(reader.read()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFilterMatchesMultiple(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, false);
    try (ParquetReader<Car> reader =
        new AvroParquetReader<>(testConf, path, column("make", equalTo("Volkswagen")))) {
      for (int i = 0; i < 10; i++) {
        assertThat(reader.read()).asString().isEqualTo(getVwPolo().toString());
        assertThat(reader.read()).asString().isEqualTo(getVwPassat().toString());
      }
      assertThat(reader.read()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFilterMatchesMultipleBlocks(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(
        10000, CompressionCodecName.UNCOMPRESSED, false, DEFAULT_BLOCK_SIZE / 64, DEFAULT_PAGE_SIZE / 64);
    try (ParquetReader<Car> reader =
        new AvroParquetReader<>(testConf, path, column("make", equalTo("Volkswagen")))) {
      for (int i = 0; i < 10000; i++) {
        assertThat(reader.read()).asString().isEqualTo(getVwPolo().toString());
        assertThat(reader.read()).asString().isEqualTo(getVwPassat().toString());
      }
      assertThat(reader.read()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFilterMatchesNoBlocks(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(
        10000, CompressionCodecName.UNCOMPRESSED, false, DEFAULT_BLOCK_SIZE / 64, DEFAULT_PAGE_SIZE / 64);
    try (ParquetReader<Car> reader = new AvroParquetReader<>(testConf, path, column("make", equalTo("Bogus")))) {
      assertThat(reader.read()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFilterMatchesFinalBlockOnly(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    Car vwPolo = getVwPolo();
    Car vwPassat = getVwPassat();
    Car bmwMini = getBmwMini();

    try (ParquetWriter<Car> writer = new AvroParquetWriter<Car>(
        path,
        Car.SCHEMA$,
        CompressionCodecName.UNCOMPRESSED,
        DEFAULT_BLOCK_SIZE / 128,
        DEFAULT_PAGE_SIZE / 128,
        false)) {
      for (int i = 0; i < 10000; i++) {
        writer.write(vwPolo);
        writer.write(vwPassat);
        writer.write(vwPolo);
      }
      writer.write(bmwMini); // only write BMW in last block
    }

    try (ParquetReader<Car> reader = new AvroParquetReader<Car>(testConf, path, column("make", equalTo("BMW")))) {
      assertThat(reader.read()).asString().isEqualTo(getBmwMini().toString());
      assertThat(reader.read()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFilterWithDictionary(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, true);
    try (ParquetReader<Car> reader =
        new AvroParquetReader<>(testConf, path, column("make", equalTo("Volkswagen")))) {
      assertThat(reader.read()).asString().isEqualTo(getVwPolo().toString());
      assertThat(reader.read()).asString().isEqualTo(getVwPassat().toString());
      assertThat(reader.read()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFilterOnSubAttribute(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, false);

    ParquetReader<Car> reader =
        new AvroParquetReader<Car>(testConf, path, column("engine.type", equalTo(EngineType.DIESEL)));
    assertThat(reader.read()).asString().isEqualTo(getVwPassat().toString());
    assertThat(reader.read()).isNull();

    reader = new AvroParquetReader<Car>(testConf, path, column("engine.capacity", equalTo(1.4f)));
    assertThat(reader.read()).asString().isEqualTo(getVwPolo().toString());
    assertThat(reader.read()).isNull();

    reader = new AvroParquetReader<Car>(testConf, path, column("engine.hasTurboCharger", equalTo(true)));
    assertThat(reader.read()).asString().isEqualTo(getBmwMini().toString());
    assertThat(reader.read()).isNull();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testProjection(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, false);
    Configuration conf = new Configuration(testConf);

    Schema schema = Car.getClassSchema();
    List<Schema.Field> fields = schema.getFields();

    // Schema.Parser parser = new Schema.Parser();
    List<Schema.Field> projectedFields = new ArrayList<Schema.Field>();
    for (Schema.Field field : fields) {
      String name = field.name();
      if ("optionalExtra".equals(name) || "serviceHistory".equals(name)) {
        continue;
      }

      // Schema schemaClone = parser.parse(field.schema().toString(false));
      Schema.Field fieldClone = new Schema.Field(name, field.schema(), field.doc(), field.defaultVal());
      projectedFields.add(fieldClone);
    }

    Schema projectedSchema =
        Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
    projectedSchema.setFields(projectedFields);
    AvroReadSupport.setRequestedProjection(conf, projectedSchema);

    try (ParquetReader<Car> reader = new AvroParquetReader<Car>(conf, path)) {
      for (Car car = reader.read(); car != null; car = reader.read()) {
        assertThat(car.getDoors()).isIn(4, 5);
        assertThat(car.getEngine()).isNotNull();
        assertThat(car.getMake()).isNotNull();
        assertThat(car.getModel()).isNotNull();
        assertThat(car.getYear()).isEqualTo(2010);
        assertThat(car.getVin()).isNotNull();
        assertThat(car.getOptionalExtra()).isNull();
        assertThat(car.getServiceHistory()).isNull();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRepeatedRecordProjection(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, false);
    Configuration conf = new Configuration(testConf);
    Schema schema = Car.getClassSchema();

    // Project a single field from repeated record schema
    final Schema projectedSchema = SchemaBuilder.builder(schema.getNamespace())
        .record("Car")
        .fields()
        .name("serviceHistory")
        .type(SchemaBuilder.unionOf()
            .nullBuilder()
            .endNull()
            .and()
            .array()
            .items(SchemaBuilder.builder(schema.getNamespace())
                .record("Service")
                .fields()
                .requiredString("mechanic")
                .endRecord())
            .endUnion())
        .noDefault()
        .endRecord();

    AvroReadSupport.setRequestedProjection(conf, projectedSchema);

    try (ParquetReader<Car> reader = new AvroParquetReader<>(conf, path)) {
      for (Car car = reader.read(); car != null; car = reader.read()) {
        assertThat(car.getServiceHistory()).isNotNull();
        car.getServiceHistory().forEach(service -> {
          assertThat(service.getMechanic()).isNotNull();
          assertThat(service.getDate()).isEqualTo(0L);
        });
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testAvroReadSchema(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, false);
    Configuration conf = new Configuration(testConf);
    AvroReadSupport.setAvroReadSchema(conf, NewCar.SCHEMA$);

    try (ParquetReader<NewCar> reader = new AvroParquetReader<>(conf, path)) {
      for (NewCar car = reader.read(); car != null; car = reader.read()) {
        assertThat(car.getEngine()).isNotNull();
        assertThat(car.getBrand()).isNotNull();
        assertThat(car.getYear()).isEqualTo(2010);
        assertThat(car.getVin()).isNotNull();
        assertThat(car.getDescription()).isNull();
        assertThat(car.getOpt()).isEqualTo(5);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testParsesSpecificDataModel(boolean compat) throws IOException {
    Configuration testConf = testConf(compat);
    // SpecificRecord contains a logical type and will fail to decode unless its SpecificData model is parsed
    List<LogicalTypesTest> records = IntStream.range(0, 25)
        .mapToObj(i -> LogicalTypesTest.newBuilder()
            .setTimestamp(Instant.now())
            .setLocalDateTime(LocalDateTimeTest.newBuilder()
                .setDate(LocalDate.now())
                .setTime(LocalTime.now())
                .build())
            .build())
        .collect(Collectors.toList());

    // Test that SpecificData model is parsed in AvroParquetWriter
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    try (ParquetWriter<LogicalTypesTest> writer = AvroParquetWriter.<LogicalTypesTest>builder(path)
        .withSchema(LogicalTypesTest.getClassSchema())
        .withConf(new Configuration(false))
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build()) {
      for (LogicalTypesTest record : records) {
        writer.write(record);
      }
    }

    // Test that SpecificData model is parsed in AvroParquetReader
    final List<LogicalTypesTest> output = new ArrayList<>();
    try (ParquetReader<org.apache.parquet.avro.LogicalTypesTest> reader = new AvroParquetReader<>(testConf, path)) {
      for (LogicalTypesTest record = reader.read(); record != null; record = reader.read()) {
        output.add(record);
      }
    }

    assertThat(output).containsExactlyElementsOf(records);
  }

  private Path writeCarsToParquetFile(int num, CompressionCodecName compression, boolean enableDictionary)
      throws IOException {
    return writeCarsToParquetFile(num, compression, enableDictionary, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
  }

  private Path writeCarsToParquetFile(
      int num, CompressionCodecName compression, boolean enableDictionary, int blockSize, int pageSize)
      throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    Car vwPolo = getVwPolo();
    Car vwPassat = getVwPassat();
    Car bmwMini = getBmwMini();

    try (ParquetWriter<Car> writer =
        new AvroParquetWriter<>(path, Car.SCHEMA$, compression, blockSize, pageSize, enableDictionary)) {
      for (int i = 0; i < num; i++) {
        writer.write(vwPolo);
        writer.write(vwPassat);
        writer.write(bmwMini);
      }
    }
    return path;
  }

  public static Car getVwPolo() {
    String vin = "WVWDB4505LK000001";
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A123 GTR")
        .setMake("Volkswagen")
        .setModel("Polo")
        .setVin(new Vin(vin.getBytes()))
        .setDoors(4)
        .setEngine(Engine.newBuilder()
            .setType(EngineType.PETROL)
            .setCapacity(1.4f)
            .setHasTurboCharger(false)
            .build())
        .setOptionalExtra(
            Stereo.newBuilder().setMake("Blaupunkt").setSpeakers(4).build())
        .setServiceHistory(ImmutableList.of(
            Service.newBuilder()
                .setDate(1325376000l)
                .setMechanic("Jim")
                .build(),
            Service.newBuilder()
                .setDate(1356998400l)
                .setMechanic("Mike")
                .build()))
        .build();
  }

  public static Car getVwPassat() {
    String vin = "WVWDB4505LK000002";
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A123 GXR")
        .setMake("Volkswagen")
        .setModel("Passat")
        .setVin(new Vin(vin.getBytes()))
        .setDoors(5)
        .setEngine(Engine.newBuilder()
            .setType(EngineType.DIESEL)
            .setCapacity(2.0f)
            .setHasTurboCharger(false)
            .build())
        .setOptionalExtra(LeatherTrim.newBuilder().setColour("Black").build())
        .setServiceHistory(ImmutableList.of(Service.newBuilder()
            .setDate(1325376000l)
            .setMechanic("Jim")
            .build()))
        .build();
  }

  public static Car getBmwMini() {
    String vin = "WBABA91060AL00003";
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A124 GSR")
        .setMake("BMW")
        .setModel("Mini")
        .setVin(new Vin(vin.getBytes()))
        .setDoors(4)
        .setEngine(Engine.newBuilder()
            .setType(EngineType.PETROL)
            .setCapacity(1.6f)
            .setHasTurboCharger(true)
            .build())
        .setOptionalExtra(null)
        .setServiceHistory(ImmutableList.of(Service.newBuilder()
            .setDate(1356998400l)
            .setMechanic("Mike")
            .build()))
        .build();
  }
}
