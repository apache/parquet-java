package parquet.bytes;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;


public class BytesInputTest {
  @Test
  public void shouldWriteZigZagEncodeData() throws IOException {
    for (int value : randomIntegers(10000)) {
      shouldWriteValue(value);
    }
    shouldWriteValue(Integer.MAX_VALUE);
    shouldWriteValue(Integer.MIN_VALUE);
    shouldWriteValue(0);
  }

  private void shouldWriteValue(int value) throws IOException {
    BytesInput b = BytesInput.fromZigZagVarInt(value);
    byte[] bytes = b.toByteArray();
    ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
    assertEquals(bytes.length, stream.available());
    int valueRead = BytesUtils.readZigZagVarInt(stream);
    assertEquals(0,stream.available());
    assertEquals(value, valueRead);
  }

  private int[] randomIntegers(int size){
    int[] data = new int[size];
    Random random =new Random();
    for(int i=0;i<size;i++){
       data[i]=random.nextInt();
    }
    return data;
  }
}
