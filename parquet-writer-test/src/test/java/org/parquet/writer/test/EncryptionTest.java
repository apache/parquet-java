/**
 * 
 */
package org.parquet.writer.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author krishnaprasad
 *
 */
public class EncryptionTest {

//	final String parquetOutputLocation = "hdfs://192.168.150.142/tmp/kp-test/new/parquet-enc-test-1.parquet";
	final String parquetOutputLocation = "/home/krishnaprasad/git/parquet-mr-1/parquet-writer-test/src/test/resources/strings-2.parquet";

	private static List<Event> events = new ArrayList<Event>(10);

	static {
		int MAX = 10;
		for (int i = 1; i <= MAX; i++) {
			final Event e = new Event();
			e.setAge(10 + i);
			e.setId(i);
			e.setEncrypted_name("KRISH-" + i);
			e.setPlace("TVC-" + i);
			events.add(e);
		}
		System.out.println("Events Created...");
	}

	// @Before
	public void checkFile() {
		File file = new File(parquetOutputLocation);
		if (file.exists()) {
			throw new RuntimeException("File already exists:" + parquetOutputLocation);
		}
	}

	@Test
	public void testReadWrite() throws IOException {
//		writeEvent();
		readEvent();
	}

	private void writeEvent() throws IOException {
		final MessageType parquetSchema = new AvroSchemaConverter().convert(Event.avroSchema);
		// create a WriteSupport object to serialize your Avro objects
		AvroWriteSupport<Event> writeSupport = new AvroWriteSupport<Event>(parquetSchema, Event.avroSchema, null);
		int blockSize = 1024;
		int pageSize = 1024;
		ParquetWriter<GenericRecord> prw = null;
		prw = new ParquetWriter(new Path(parquetOutputLocation), ParquetFileWriter.Mode.CREATE, writeSupport,
				CompressionCodecName.SNAPPY, blockSize, pageSize, pageSize, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
				ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED, WriterVersion.PARQUET_2_0, new Configuration());
		long t = System.currentTimeMillis();
		for (Event e : events) {
			GenericRecord record = ParquetEventTranslator.translate(e);
			prw.write(record);
		}
		prw.close();
		System.out.println("**********TT:" + (System.currentTimeMillis() - t));
	}

	private void readEvent() throws IOException {
		AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
		ParquetReader<GenericRecord> pr = ParquetReader.builder(readSupport, new Path(parquetOutputLocation)).build();
		for (Event e : events) {
			GenericRecord gr = pr.read();
			Event event = ParquetEventTranslator.translate(gr);
			Assert.assertEquals(e, event);
			System.out.println("Event Actual:" + e + ", Event Got:" + event);
		}
		pr.close();
		System.out.println("Test Ran Successfully...");
	}

	// @After
	public void deleteFile() {
		File file = new File(parquetOutputLocation);
		if (file.exists()) {
			file.delete();
		}
		System.out.println("File deleted:" + parquetOutputLocation);
	}
}
