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
package parquet.avro;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import parquet.Log;
import parquet.column.ColumnReader;
import parquet.filter.ColumnPredicates;
import parquet.filter.ColumnRecordFilter;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;

public class TestSpecificInputOutputFormat {
  private static final Log LOG = Log.getLog(TestSpecificInputOutputFormat.class);

  public static Car nextRecord(int i) {
    String vin = "1VXBR12EXCP000000";
    Car.Builder carBuilder = Car.newBuilder()
        .setDoors(2)
        .setMake("Tesla")
        .setModel(String.format("Model X v%d", i % 2))
        .setVin(new Vin(vin.getBytes()))
        .setYear(2014 + i)
        .setOptionalExtra(LeatherTrim.newBuilder().setColour("black").build())
        .setRegistration("California");
    Engine.Builder engineBuilder = Engine.newBuilder()
        .setCapacity(85.0f)
        .setHasTurboCharger(false);
    if (i % 2 == 0) {
      engineBuilder.setType(EngineType.ELECTRIC);
    } else {
      engineBuilder.setType(EngineType.PETROL);
    }
    carBuilder.setEngine(engineBuilder.build());
    if (i % 4 == 0) {
      List<Service> serviceList = Lists.newArrayList();
      serviceList.add(Service.newBuilder()
          .setDate(1374084640)
          .setMechanic("Elon Musk").build());
      carBuilder.setServiceHistory(serviceList);
    }
    return carBuilder.build();
  }

  public static class MyMapper extends Mapper<LongWritable, Text, Void, Car> {

    @Override
    public void run(Context context) throws IOException ,InterruptedException {
      for (int i = 0; i < 10; i++) {
        context.write(null, nextRecord(i));
      }
    }
  }

  public static class MyMapper2 extends Mapper<Void, Car, Void, Car> {
    @Override
    protected void map(Void key, Car car, Context context) throws IOException ,InterruptedException {
      // Note: Car can be null because of predicate pushdown defined by an UnboundedRecordFilter (see below)
      if (car != null) {
        context.write(null, car);
      }
    }

  }

  public static class MyMapperShort extends
      Mapper<Void, ShortCar, Void, ShortCar> {
    @Override
    protected void map(Void key, ShortCar car, Context context)
        throws IOException, InterruptedException {
      // Note: Car can be null because of predicate pushdown defined by an
      // UnboundedRecordFilter (see below)
      if (car != null) {
        context.write(null, car);
      }
    }

  }

  public static class ElectricCarFilter implements UnboundRecordFilter {

    private final UnboundRecordFilter filter;

    public ElectricCarFilter() {
      filter = ColumnRecordFilter.column("engine.type", ColumnPredicates.equalTo(EngineType.ELECTRIC));
    }

    @Override
    public RecordFilter bind(Iterable<ColumnReader> readers) {
      return filter.bind(readers);
    }
  }

  final Configuration conf = new Configuration();
  final Path inputPath = new Path("src/test/java/parquet/avro/TestSpecificInputOutputFormat.java");
  final Path parquetPath = new Path("target/test/hadoop/TestSpecificInputOutputFormat/parquet");
  final Path outputPath = new Path("target/test/hadoop/TestSpecificInputOutputFormat/out");

  @Before
  public void createParquetFile() throws Exception {
    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      final Job job = new Job(conf, "write");

      // input not really used
      TextInputFormat.addInputPath(job, inputPath);
      job.setInputFormatClass(TextInputFormat.class);

      job.setMapperClass(TestSpecificInputOutputFormat.MyMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(AvroParquetOutputFormat.class);
      AvroParquetOutputFormat.setOutputPath(job, parquetPath);
      AvroParquetOutputFormat.setSchema(job, Car.SCHEMA$);

      waitForJob(job);
    }
  }
  
  @Test
  public void testReadWrite() throws Exception {

    final Job job = new Job(conf, "read");
    job.setInputFormatClass(AvroParquetInputFormat.class);
    AvroParquetInputFormat.setInputPaths(job, parquetPath);
    // Test push-down predicates by using an electric car filter
    AvroParquetInputFormat.setUnboundRecordFilter(job, ElectricCarFilter.class);

    // Test schema projection by dropping the optional extras
    Schema projection = Schema.createRecord(Car.SCHEMA$.getName(),
        Car.SCHEMA$.getDoc(), Car.SCHEMA$.getNamespace(), false);
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : Car.SCHEMA$.getFields()) {
      if (!"optionalExtra".equals(field.name())) {
        fields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
            field.defaultValue(), field.order()));
      }
    }
    projection.setFields(fields);
    AvroParquetInputFormat.setRequestedProjection(job, projection);

    job.setMapperClass(TestSpecificInputOutputFormat.MyMapper2.class);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    AvroParquetOutputFormat.setOutputPath(job, outputPath);
    AvroParquetOutputFormat.setSchema(job, Car.SCHEMA$);

    waitForJob(job);

    final Path mapperOutput = new Path(outputPath.toString(),
        "part-m-00000.parquet");
    final AvroParquetReader<Car> out = new AvroParquetReader<Car>(mapperOutput);
    Car car;
    Car previousCar = null;
    int lineNumber = 0;
    while ((car = out.read()) != null) {
      if (previousCar != null) {
         // Testing reference equality here. The "model" field should be dictionary-encoded.
         assertTrue(car.getModel() == previousCar.getModel());
      }
      // Make sure that predicate push down worked as expected
      if (car.getEngine().getType() == EngineType.PETROL) {
        fail("UnboundRecordFilter failed to remove cars with PETROL engines");
      }
      // Note we use lineNumber * 2 because of predicate push down
      Car expectedCar = nextRecord(lineNumber * 2);
      // We removed the optional extra field using projection so we shouldn't
      // see it here...
      expectedCar.setOptionalExtra(null);
      assertEquals("line " + lineNumber, expectedCar, car);
      ++lineNumber;
      previousCar = car;
    }
    out.close();
  }

  @Test
  public void testReadWriteChangedCar() throws Exception {

    final Job job = new Job(conf, "read changed/short");
    job.setInputFormatClass(AvroParquetInputFormat.class);
    AvroParquetInputFormat.setInputPaths(job, parquetPath);
    // Test push-down predicates by using an electric car filter
    AvroParquetInputFormat.setUnboundRecordFilter(job, ElectricCarFilter.class);

    // Test schema projection by dropping the engine, year, and vin (like ShortCar),
    // but making make optional (unlike ShortCar)
    Schema projection = Schema.createRecord(Car.SCHEMA$.getName(),
        Car.SCHEMA$.getDoc(), Car.SCHEMA$.getNamespace(), false);
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : Car.SCHEMA$.getFields()) {
      // No make!
      if ("engine".equals(field.name()) || "year".equals(field.name()) || "vin".equals(field.name())) {
        fields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
            field.defaultValue(), field.order()));
      }
    }
    projection.setFields(fields);
    AvroParquetInputFormat.setRequestedProjection(job, projection);
    AvroParquetInputFormat.setAvroReadSchema(job, ShortCar.SCHEMA$);

    job.setMapperClass(TestSpecificInputOutputFormat.MyMapperShort.class);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    AvroParquetOutputFormat.setOutputPath(job, outputPath);
    AvroParquetOutputFormat.setSchema(job, ShortCar.SCHEMA$);

    waitForJob(job);

    final Path mapperOutput = new Path(outputPath.toString(), "part-m-00000.parquet");
    final AvroParquetReader<ShortCar> out = new AvroParquetReader<ShortCar>(mapperOutput);
    ShortCar car;
    int lineNumber = 0;
    while ((car = out.read()) != null) {
      // Make sure that predicate push down worked as expected
      // Note we use lineNumber * 2 because of predicate push down
      Car expectedCar = nextRecord(lineNumber * 2);
      // We removed the optional extra field using projection so we shouldn't see it here...
      assertNull(car.getMake());
      assertEquals(car.getEngine(), expectedCar.getEngine());
      assertEquals(car.getYear(), expectedCar.getYear());
      assertEquals(car.getVin(), expectedCar.getVin());
      ++lineNumber;
    }
    out.close();
  }

  private void waitForJob(Job job) throws Exception {
    job.submit();
    while (!job.isComplete()) {
      LOG.debug("waiting for job " + job.getJobName());
      sleep(100);
    }
    LOG.info("status for job " + job.getJobName() + ": " + (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }

  @After
  public void deleteOutputFile() throws IOException {
    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
  }
}
