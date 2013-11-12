package parquet.column.values.deltalengthbytearray;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import parquet.io.api.Binary;

public class TestDeltaLengthByteArray {
	
	@Test
	public void testSerialization () throws IOException {
		DeltaLengthByteArrayValuesWriter writer = new DeltaLengthByteArrayValuesWriter(64*1024);
		
		Binary b1 = Binary.fromString("parquet");
		Binary b2 = Binary.fromString("hadoop");
		Binary b3 = Binary.fromString("mapreduce");
		
		writer.writeBytes(b1);
		writer.writeBytes(b2);
		writer.writeBytes(b3);
		
		DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();
		reader.initFromPage(3, writer.getBytes().toByteArray(), 0);
		
		Assert.assertTrue(reader.readBytes().equals(b1));
    Assert.assertTrue(reader.readBytes().equals(b2));
    Assert.assertTrue(reader.readBytes().equals(b3));
	}
}
