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
package redelm.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>Encodes signed and unsigned values using a common variable-length
 * scheme, found for example in
 * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
 * Google's Protocol Buffers</a>. It uses fewer bytes to encode smaller values,
 * but will use slightly more bytes to encode large values.</p>
 *
 * <p>Signed values are further encoded using so-called zig-zag encoding
 * in order to make them "compatible" with variable-length encoding.</p>
 *
 * <p>This is taken from mahout-core, and is included to avoid having to pull
 * in the entirety of Mahout.</p>
 */
public class Varint {
    private Varint() {}

    /**
     * Encodes a value using the variable-length encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. It uses zig-zag encoding to efficiently
     * encode signed values. If values are known to be nonnegative,
     * {@link #writeUnsignedVarLong(long, DataOutput)} should be used.
     *
     * @param value value to encode
     * @param out to write bytes to
     * @throws IOException if {@link DataOutput} throws {@link IOException}
     */
    public static void writeSignedVarLong(long value, DataOutput out) throws IOException {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        writeUnsignedVarLong((value << 1) ^ (value >> 63), out);
    }

    /**
     * Encodes a value using the variable-length encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative.
     * If values can be negative, use {@link #writeSignedVarLong(long, DataOutput)}
     * instead. This method treats negative input as like a large unsigned value.
     *
     * @param value value to encode
     * @param out to write bytes to
     * @throws IOException if {@link DataOutput} throws {@link IOException}
     */
    public static void writeUnsignedVarLong(long value, DataOutput out) throws IOException {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            out.writeByte(((int) value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte((int) value & 0x7F);
    }

    /**
     * @see #writeSignedVarLong(long, DataOutput)
     */
    public static void writeSignedVarInt(int value, DataOutput out) throws IOException {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        writeUnsignedVarInt((value << 1) ^ (value >> 31), out);
    }

    /**
     * @see #writeUnsignedVarLong(long, DataOutput)
     */
    public static void writeUnsignedVarInt(int value, DataOutput out) throws IOException {
        while ((value & 0xFFFFFF80) != 0L) {
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value & 0x7F);
    }

    /**
     * @param in to read bytes from
     * @return decode value
     * @throws IOException if {@link DataInput} throws {@link IOException}
     * @throws IllegalArgumentException if variable-length value does not terminate
     *  after 9 bytes have been read
     * @see #writeSignedVarLong(long, DataOutput)
     */
    public static long readSignedVarLong(DataInput in) throws IOException {
        long raw = readUnsignedVarLong(in);
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    /**
     * @param in to read bytes from
     * @return decode value
     * @throws IOException if {@link DataInput} throws {@link IOException}
     * @throws IllegalArgumentException if variable-length value does not terminate
     *  after 9 bytes have been read
     * @see #writeUnsignedVarLong(long, DataOutput)
     */
    public static long readUnsignedVarLong(DataInput in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new RuntimeException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    /**
     * @throws IllegalArgumentException if variable-length value does not terminate
     *  after 5 bytes have been read
     * @throws IOException if {@link DataInput} throws {@link IOException}
     * @see #readSignedVarLong(DataInput)
     */
    public static int readSignedVarInt(DataInput in) throws IOException {
        int raw = readUnsignedVarInt(in);
        // This undoes the trick in writeSignedVarInt()
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1 << 31));
    }

    /**
     * @throws IllegalArgumentException if variable-length value does not terminate
     *  after 5 bytes have been read
     * @throws IOException if {@link DataInput} throws {@link IOException}
     * @see #readUnsignedVarLong(DataInput)
     */
    public static int readUnsignedVarInt(DataInput in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) {
                throw new RuntimeException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }
}