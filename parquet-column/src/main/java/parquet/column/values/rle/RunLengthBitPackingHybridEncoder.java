package parquet.column.values.rle;

import java.io.IOException;

import parquet.Log;
import parquet.Preconditions;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.values.bitpacking.ByteBitPacking;
import parquet.column.values.bitpacking.BytePacker;

import static parquet.Log.DEBUG;

/**
 * Encodes values using a combination of run length encoding and bit packing,
 * according to the following grammar:
 *
 * <code>
 * encoded-block := <run>*
 * run := <bit-packed-run> | <rle-run>
 * bit-packed-run := <bit-packed-header> <bit-packed-values>
 * bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
 * // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
 * bit-pack-count := (number of values in this run) / 8
 * bit-packed-values :=  bit packed back to back, from LSB to MSB
 * rle-run := <rle-header> <repeated-value>
 * rle-header := varint-encode( (number of times repeated) << 1)
 * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
 * </code>
 *
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridEncoder {
  private static final Log LOG = Log.getLog(RunLengthBitPackingHybridEncoder.class);

  /**
   * These represent how many repeats are needed to use RLE.
   * These values are optimal when we expect to frequently
   * see 504 values (the max bit-packed-run length) without switching
   * to RLE. These thresholds could be lowered if we expect repeats
   * to occur more frequently. Everything with bitWidth > 12 however
   * should use RLE whenever it can.
   */
  private static final int[] RLE_THRESHOLDS = new int[] {16, 8, 6, 4, 4, 3, 3, 2, 3, 3, 3};

  private final BytePacker packer;

  private final CapacityByteArrayOutputStream baos;

  /**
   * The bit width used for bit-packing and for writing
   * the repeated-value
   */
  private final int bitWidth;

  /**
   * How many times a value needs to repeat in order
   * to use run length encoding instead of bit packing.
   * Changes based on bitWidth.
   */
  private final int rleThreshold;

  /**
   * Values that are bit packed 8 at at a time are packed into this
   * buffer, which is then written to baos
   */
  private final byte[] packBuffer;

  /**
   * Previous value written, used to detect repeated values
   */
  private int previousValue;

  /**
   * We buffer 8 values at a time, and either bit pack them
   * or discard them after writing a rle-run
   */
  private final int[] bufferedValues;
  private int numBufferedValues;

  /**
   * How many times a value has been repeated
   */
  private int repeatCount;

  /**
   * How many groups of 8 values have been written
   * to the current bit-packed-run
   */
  private int bitPackedGroupCount;

  /**
   * A "pointer" to a single byte in baos,
   * which we use as our bit-packed-header. It's really
   * the logical index of the byte in baos.
   *
   * We are only using one byte for this header,
   * which limits us to writing 504 values per bit-packed-run.
   *
   * MSB must be 0 for varint encoding, LSB must be 1 to signify
   * that this is a bit-packed-header leaves 6 bits to write the
   * number of 8-groups -> (2^6 - 1) * 8 = 504
   */
  private long bitPackedRunHeaderPointer;

  private boolean toBytesCalled;

  public RunLengthBitPackingHybridEncoder(int bitWidth, int initialCapacity, int rleThreshold) {
    if (DEBUG) {
      LOG.debug(String.format("Encoding: RunLengthBitPackingHybridEncoder with "
        + "bithWidth: %d initialCapacity %d", bitWidth, initialCapacity));
    }

    Preconditions.checkArgument(bitWidth > 0 && bitWidth <= 32, "bitWidth must be > 0 and <= 32");

    this.bitWidth = bitWidth;
    this.baos = new CapacityByteArrayOutputStream(initialCapacity);
    this.packBuffer = new byte[bitWidth];
    this.bufferedValues = new int[rleThreshold];
    this.packer = ByteBitPacking.getPacker(bitWidth);
    this.rleThreshold = rleThreshold;

    reset(false);
  }

  public RunLengthBitPackingHybridEncoder(int bitWidth, int initialCapacity) {
    this(bitWidth, initialCapacity, determineRleThreshold(bitWidth));
  }

  private static int determineRleThreshold(int bitWidth) {
    if (bitWidth - 1 < RLE_THRESHOLDS.length) {
      return RLE_THRESHOLDS[bitWidth - 1];
    }
    return 2;
  }

  private void reset(boolean resetBaos) {
    if (resetBaos) {
      this.baos.reset();
    }
    this.previousValue = 0;
    this.numBufferedValues = 0;
    this.repeatCount = 0;
    this.bitPackedGroupCount = 0;
    this.bitPackedRunHeaderPointer = -1;
    this.toBytesCalled = false;
  }

  public void writeInt(int value) throws IOException {
    if (value == previousValue) {
      // keep track of how many times we've seen this value
      // consecutively
      ++repeatCount;

      if (repeatCount >= rleThreshold) {
        // we've seen this at least 8 times, we're
        // certainly going to write an rle-run,
        // so just keep on counting repeats for now
        return;
      }
    } else {
      // This is a new value, check if it signals the end of
      // an rle-run
      if (repeatCount >= rleThreshold) {
        // it does! write an rle-run
        writeRleRun();
      }

      // this is a new value so we've only seen it once
      repeatCount = 1;
      // start tracking this value for repeats
      previousValue = value;
    }

    // We have not seen enough repeats to justify an rle-run yet,
    // so buffer this value in case we decide to write a bit-packed-run
    bufferedValues[numBufferedValues] = value;
    ++numBufferedValues;

    if (numBufferedValues == 8) {
      // we've encountered less than 8 repeated values, so
      // either start a new bit-packed-run or append to the
      // current bit-packed-run
      writeOrAppendBitPackedRun();
    }
  }

  private void writeOrAppendBitPackedRun() throws IOException {
    if (bitPackedGroupCount >= 63) {
      // we've packed as many values as we can for this run,
      // end it and start a new one
      endPreviousBitPackedRun();
    }

    if (bitPackedRunHeaderPointer == -1) {
      // this is a new bit-packed-run, allocate a byte for the header
      // and keep a "pointer" to it so that it can be mutated later
      baos.write(0); // write a sentinel value
      bitPackedRunHeaderPointer = baos.getCurrentIndex();
    }

    packer.pack8Values(bufferedValues, 0, packBuffer, 0);
    baos.write(packBuffer);

    // empty the buffer, they've all bee written
    numBufferedValues = 0;

    // clear the repeat count, as some repeated values
    // may have just been bit packed into this run
    repeatCount = 0;

    ++bitPackedGroupCount;
  }

  /**
   * If we are currently writing a bit-packed-run, update the
   * bit-packed-header and consider this run to be over
   *
   * does nothing if we're not currently writing a bit-packed run
   */
  private void endPreviousBitPackedRun() {
    if (bitPackedRunHeaderPointer == -1) {
      // we're not currently in a bit-packed-run
      return;
    }

    // create bit-packed-header, which needs to fit in 1 byte
    byte bitPackHeader = (byte) ((bitPackedGroupCount << 1) | 1);

    // update this byte
    baos.setByte(bitPackedRunHeaderPointer, bitPackHeader);

    // mark that this run is over
    bitPackedRunHeaderPointer = -1;

    // reset the number of groups
    bitPackedGroupCount = 0;
  }

  private void writeRleRun() throws IOException {
    endPreviousBitPackedRun();

    // write the rle-header (lsb of 0 signifies a rle run)
    BytesUtils.writeUnsignedVarInt(repeatCount << 1, baos);
    // write the repeated-value
    BytesUtils.writeIntLittleEndianPaddedOnBitWidth(baos, previousValue, bitWidth);

    // reset the repeat count
    repeatCount = 0;

    // throw away all the buffered values, they were just repeats and they've been written
    numBufferedValues = 0;
  }

  public BytesInput toBytes() throws IOException {
    Preconditions.checkArgument(!toBytesCalled,
        "You cannot call toBytes() more than once without calling reset()");

    if (repeatCount >= rleThreshold) {
      writeRleRun();
    } else if(numBufferedValues > 0) {
      for (int i = numBufferedValues; i < 8; i++) {
        bufferedValues[i] = 0;
      }

      writeOrAppendBitPackedRun();
      endPreviousBitPackedRun();
    } else {
      endPreviousBitPackedRun();
    }

    toBytesCalled = true;
    return BytesInput.from(baos);
  }

  /**
   * Reset this encoder for re-use
   */
  public void reset() {
    reset(true);
  }
}
