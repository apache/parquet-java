package org.apache.parquet.hadoop;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.codec.CleanUtil;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test to check how Parquet writing behaves in case of an error happens during the writes. We use an OOM because
 * that is the most tricky to handle. In this case we shall avoid flushing since it may cause writing to already
 * released memory spaces.
 * <p>
 * To catch the potential issue of writing into released ByteBuffer objects, direct memory allocation is used and at the
 * release() call we actually release the related direct memory and zero the address inside the ByteBuffer object. As a
 * result, a subsequent read/write call on the related ByteBuffer object will crash the whole jvm. (Unfortunately, there
 * is no better way to test this.) To avoid crashing the test executor jvm, the code of this test is executed in a
 * separate process.
 */
public class TestParquetWriterError {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testInSeparateProcess() throws IOException, InterruptedException {
    String outputFile = tmpFolder.newFile("out.parquet").toString();

    String classpath = System.getProperty("java.class.path");
    String javaPath = Paths.get(System.getProperty("java.home"), "bin", "java").toAbsolutePath().toString();
    Process process = new ProcessBuilder()
        .command(
            javaPath,
            "-cp",
            classpath,
            Main.class.getName(),
            outputFile)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
        .start();
    Assert.assertEquals("Test process exited with a non-zero return code. See previous logs for details.", 0,
        process.waitFor());
  }

  /**
   * The class to be used to execute this test in a separate thread.
   */
  public static class Main {

    private static final Random RANDOM = new Random(2024_02_27_14_20L);

    // See the release() implementation in createAllocator()
    private static final Field BUFFER_ADDRESS;

    static {
      try {
        Class<?> bufferClass = Class.forName("java.nio.Buffer");
        BUFFER_ADDRESS = bufferClass.getDeclaredField("address");
        BUFFER_ADDRESS.setAccessible(true);
      } catch (ClassNotFoundException | NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }

    private static Group generateNext() {
      PhoneBookWriter.Location location;
      double chance = RANDOM.nextDouble();
      if (chance < .45) {
        location = new PhoneBookWriter.Location(RANDOM.nextDouble(), RANDOM.nextDouble());
      } else if (chance < .9) {
        location = new PhoneBookWriter.Location(RANDOM.nextDouble(), null);
      } else {
        location = null;
      }
      List<PhoneBookWriter.PhoneNumber> phoneNumbers;
      if (RANDOM.nextDouble() < .1) {
        phoneNumbers = null;
      } else {
        int n = RANDOM.nextInt(4);
        phoneNumbers = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
          String kind = RANDOM.nextDouble() < .1 ? null : "kind" + RANDOM.nextInt(5);
          phoneNumbers.add(new PhoneBookWriter.PhoneNumber(RANDOM.nextInt(), kind));
        }
      }
      String name = RANDOM.nextDouble() < .1 ? null : "name" + RANDOM.nextLong();
      PhoneBookWriter.User user = new PhoneBookWriter.User(RANDOM.nextLong(), name, phoneNumbers, location);
      return PhoneBookWriter.groupFromUser(user);
    }

    private static TrackingByteBufferAllocator createAllocator(final int oomAt) {
      return TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator() {
        private int counter = 0;

        @Override
        public ByteBuffer allocate(int size) {
          if (++counter >= oomAt) {
            Assert.assertEquals("There should not be any additional allocations after an OOM", oomAt, counter);
            throw new OutOfMemoryError("Artificial OOM to fail write");
          }
          return super.allocate(size);
        }

        @Override
        public void release(ByteBuffer b) {
          CleanUtil.cleanDirectBuffer(b);

          // It seems, if the size of the buffers are small, the related memory space is not given back to the OS, so
          // writing to them after release does not cause any identifiable issue. Therefore, we explicitly zero the
          // address, so the jvm crashes for a subsequent access.
          try {
            BUFFER_ADDRESS.setLong(b, 0L);
          } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to zero direct ByteBuffer address", e);
          }
        }
      });
    }

    public static void main(String[] args) throws Throwable {
      CompressionCodecName[] codecs = {
          CompressionCodecName.UNCOMPRESSED,
          CompressionCodecName.GZIP,
          CompressionCodecName.SNAPPY,
          CompressionCodecName.ZSTD,
          CompressionCodecName.LZ4_RAW};
      for (int cycle = 0; cycle < 50; ++cycle) {
        try (TrackingByteBufferAllocator allocator = createAllocator(RANDOM.nextInt(100) + 1);
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(Paths.get(args[0])))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withType(PhoneBookWriter.getSchema())
                .withAllocator(allocator)
                .withCodecFactory(CodecFactory.createDirectCodecFactory(
                    new Configuration(),
                    allocator,
                    ParquetProperties.DEFAULT_PAGE_SIZE))
                .withCompressionCodec(codecs[RANDOM.nextInt(codecs.length)])
                .build()) {
          for (int i = 0; i < 100_000; ++i) {
            writer.write(generateNext());
          }
          Assert.fail("An OOM should have been thrown");
        } catch (OutOfMemoryError oom) {
          Throwable[] suppressed = oom.getSuppressed();
          // No exception should be suppressed after the expected OOM:
          // It would mean that a close() call fails with an exception
          if (suppressed != null && suppressed.length > 0) {
            throw suppressed[0];
          }
        }
      }
    }
  }
}
