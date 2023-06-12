package org.apache.parquet.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * {@code DiskInputFile} is an implementation needed by Parquet to read
 * data files from disk using {@link SeekableInputStream} instances.
 */
public class DiskInputFile implements InputFile {

  private final Path path;

  public DiskInputFile(Path file) {
    path = file;
  }

  /**
   * @return the total length of the file, in bytes.
   * @throws IOException if the length cannot be determined
   */
  @Override
  public long getLength() throws IOException {
    RandomAccessFile file = new RandomAccessFile(path.toFile(), "r");
    long length = file.length();
    file.close();
    return length;
  }

  /**
   * Open a new {@link SeekableInputStream} for the underlying data file.
   *
   * @return a new {@link SeekableInputStream} to read the file
   * @throws IOException if the stream cannot be opened
   */
  @Override
  public SeekableInputStream newStream() throws IOException {

    return new SeekableInputStream() {

      final RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "r");

      /**
       * Reads the next byte of data from the input stream. The value byte is
       * returned as an {@code int} in the range {@code 0} to
       * {@code 255}. If no byte is available because the end of the stream
       * has been reached, the value {@code -1} is returned. This method
       * blocks until input data is available, the end of the stream is detected,
       * or an exception is thrown.
       *
       * @return     the next byte of data, or {@code -1} if the end of the
       *             stream is reached.
       * @throws     IOException  if an I/O error occurs.
       */
      @Override
      public int read() throws IOException {
        return randomAccessFile.read();
      }

      /**
       * Return the current position in the InputStream.
       *
       * @return current position in bytes from the start of the stream
       */
      @Override
      public long getPos() throws IOException {
        return randomAccessFile.getFilePointer();
      }

      /**
       * Seek to a new position in the InputStream.
       *
       * @param newPos the new position to seek to
       * @throws IOException If the underlying stream throws IOException
       */
      @Override
      public void seek(long newPos) throws IOException {
        randomAccessFile.seek(newPos);
      }

      /**
       * Read a byte array of data, from position 0 to the end of the array.
       * <p>
       * This method is equivalent to {@code read(bytes, 0, bytes.length)}.
       * <p>
       * This method will block until len bytes are available to copy into the
       * array, or will throw {@link EOFException} if the stream ends before the
       * array is full.
       *
       * @param bytes a byte array to fill with data from the stream
       * @throws IOException If the underlying stream throws IOException
       * @throws EOFException If the stream has fewer bytes left than are needed to
       *                      fill the array, {@code bytes.length}
       */
      @Override
      public void readFully(byte[] bytes) throws IOException {
        randomAccessFile.readFully(bytes);
      }

      /**
       * Read {@code len} bytes of data into an array, at position {@code start}.
       * <p>
       * This method will block until len bytes are available to copy into the
       * array, or will throw {@link EOFException} if the stream ends before the
       * array is full.
       *
       * @param bytes a byte array to fill with data from the stream
       * @param start the starting position in the byte array for data
       * @param len the length of bytes to read into the byte array
       * @throws IOException If the underlying stream throws IOException
       * @throws EOFException If the stream has fewer than {@code len} bytes left
       */
      @Override
      public void readFully(byte[] bytes, int start, int len) throws IOException {
        randomAccessFile.readFully(bytes, start, len);
      }

      /**
       * Read {@code buf.remaining()} bytes of data into a {@link ByteBuffer}.
       * <p>
       * This method will copy available bytes into the buffer, reading at most
       * {@code buf.remaining()} bytes. The number of bytes actually copied is
       * returned by the method, or -1 is returned to signal that the end of the
       * underlying stream has been reached.
       *
       * @param buf a byte buffer to fill with data from the stream
       * @return the number of bytes read or -1 if the stream ended
       * @throws IOException If the underlying stream throws IOException
       */
      @Override
      public int read(ByteBuffer buf) throws IOException {
        byte[] buffer = new byte[buf.remaining()];
        int code = read(buffer);
        buf.put(buffer, buf.position() + buf.arrayOffset(), buf.remaining());
        return code;
      }

      /**
       * Read {@code buf.remaining()} bytes of data into a {@link ByteBuffer}.
       * <p>
       * This method will block until {@code buf.remaining()} bytes are available
       * to copy into the buffer, or will throw {@link EOFException} if the stream
       * ends before the buffer is full.
       *
       * @param buf a byte buffer to fill with data from the stream
       * @throws IOException If the underlying stream throws IOException
       * @throws EOFException If the stream has fewer bytes left than are needed to
       *                      fill the buffer, {@code buf.remaining()}
       */
      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        byte[] buffer = new byte[buf.remaining()];
        readFully(buffer);
        buf.put(buffer, buf.position() + buf.arrayOffset(), buf.remaining());
      }

      /**
       * Closes the resource.
       * @throws IOException if the underlying resource throws an IOException
       */
      @Override
      public void close() throws IOException {
        randomAccessFile.close();
      }
    };
  }
}
