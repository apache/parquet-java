package parquet.avro;

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
import static parquet.filter.ColumnRecordFilter.equalTo;
import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

public class TestSpecificReadWrite {

  @Test
  public void testReadWriteSpecific() throws IOException {
    Path path = writeCarsToParquetFile( 10, false, CompressionCodecName.UNCOMPRESSED, false);
    ParquetReader<Car> reader = new AvroParquetReader<Car>(path);
    for ( int i =0; i < 10; i++ ) {
      assertEquals(getVwPolo().toString(), reader.read().toString());
      assertEquals(getVwPassat().toString(), reader.read().toString());
      assertEquals(getBmwMini().toString(), reader.read().toString());
    }
    assertNull(reader.read());
  }

  @Test
  public void testFilterMatchesMultiple() throws IOException {

    Path path = writeCarsToParquetFile(10, false, CompressionCodecName.UNCOMPRESSED, false);

    ParquetReader<Car> reader = new AvroParquetReader<Car>(path, column("make", equalTo("Volkswagen")));
    for ( int i =0; i < 10; i++ ) {
      assertEquals(reader.read().toString(), getVwPolo().toString());
      assertEquals(reader.read().toString(), getVwPassat().toString());
    }
    assertNull( reader.read());
  }


  private Path writeCarsToParquetFile( int num, boolean varyYear,
                                      CompressionCodecName compression, boolean enableDictionary) throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    Car vwPolo    =  getVwPolo();
    Car vwPassat  =  getVwPassat();
    Car bmwMini   =  getBmwMini();

    ParquetWriter<Car> writer = new AvroParquetWriter<Car>(path, Car.SCHEMA$, compression,
        DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE,enableDictionary);
    for ( int i =0; i < num; i++ ) {
      if (varyYear ) {
        vwPolo.setYear( ( i / 100l ));
        vwPassat.setYear( ( i / 100l ));
        bmwMini.setYear( ( i / 100l ));
      }
      writer.write(vwPolo);
      writer.write(vwPassat);
      writer.write(bmwMini);
    }
    writer.close();
    return path;
  }

  public static Car getVwPolo() {
    return Car.newBuilder()
        .setYear(2010)
        .setMake("Volkswagen")
        .setModel("Polo")
        .setDoors(4)
        .setEngineCapacity(1.4f)
        .build();
  }

  public static Car getVwPassat() {
    return Car.newBuilder()
        .setYear(2010)
        .setMake("Volkswagen")
        .setModel("Passat")
        .setDoors(5)
        .setEngineCapacity(2.0f)
        .build();
  }

  public static Car getBmwMini() {
    return Car.newBuilder()
        .setYear(2010)
        .setMake("BMW")
        .setModel("Mini")
        .setDoors(4)
        .setEngineCapacity(1.6f)
        .build();
  }
}
