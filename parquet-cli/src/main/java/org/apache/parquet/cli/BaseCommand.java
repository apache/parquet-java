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

package org.apache.parquet.cli;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.cli.json.AvroJsonReader;
import org.apache.parquet.cli.util.Formats;
import org.apache.parquet.cli.util.GetClassLoader;
import org.apache.parquet.cli.util.Schemas;
import org.apache.parquet.cli.util.SeekableFSDataInputStream;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;

public abstract class BaseCommand implements Command, Configurable {

  private static final String RESOURCE_URI_SCHEME = "resource";
  private static final String STDIN_AS_SOURCE = "stdin";
  public static final String PARQUET_CLI_ENABLE_GROUP_READER = "parquet.enable.simple-reader";

  /**
   * Note for dev: Due to legancy reasons, parquet-cli used the avro schema reader which
   * breaks for files generated through proto. This logic is in place to auto-detect such cases
   * and route the request to simple reader instead of avro.
   */
  private boolean isProtobufStyleSchema(String source) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(getConf(), qualifiedPath(source))) {
      Map<String, String> metadata = reader.getFooter().getFileMetaData().getKeyValueMetaData();
      return metadata != null && metadata.containsKey("parquet.proto.class");
    }
  }

  // Util to convert ParquetReader to Iterable
  private static <T> Iterable<T> asIterable(final ParquetReader<T> reader) {
    return () -> new Iterator<T>() {
      private T next = advance();

      private T advance() {
        try {
          return reader.read();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public T next() {
        T current = next;
        next = advance();
        return current;
      }
    };
  }

  protected final Logger console;

  private Configuration conf = null;
  private LocalFileSystem localFS = null;

  public BaseCommand(Logger console) {
    this.console = console;
  }

  /**
   * @return FileSystem to use when no file system scheme is present in a path
   * @throws IOException if there is an error loading the default fs
   */
  public FileSystem defaultFS() throws IOException {
    if (localFS == null) {
      this.localFS = FileSystem.getLocal(getConf());
    }
    return localFS;
  }

  /**
   * Output content to the console or a file.
   * <p>
   * This will not produce checksum files.
   *
   * @param content  String content to write
   * @param console  A {@link Logger} for writing to the console
   * @param filename The destination {@link Path} as a String
   * @throws IOException if there is an error while writing
   */
  public void output(String content, Logger console, String filename) throws IOException {
    if (filename == null || "-".equals(filename)) {
      console.info(content);
    } else {
      try (FSDataOutputStream outgoing = create(filename)) {
        outgoing.write(content.getBytes(StandardCharsets.UTF_8));
        outgoing.flush();
      }
    }
  }

  /**
   * Creates a file and returns an open {@link FSDataOutputStream}.
   * <p>
   * If the file does not have a file system scheme, this uses the default FS.
   * <p>
   * This will not produce checksum files and will overwrite a file that
   * already exists.
   *
   * @param filename The filename to create
   * @return An open FSDataOutputStream
   * @throws IOException if there is an error creating the file
   */
  public FSDataOutputStream create(String filename) throws IOException {
    return create(filename, true);
  }

  /**
   * Creates a file and returns an open {@link FSDataOutputStream}.
   * <p>
   * If the file does not have a file system scheme, this uses the default FS.
   * <p>
   * This will produce checksum files and will overwrite a file that already
   * exists.
   *
   * @param filename The filename to create
   * @return An open FSDataOutputStream
   * @throws IOException if there is an error creating the file
   */
  public FSDataOutputStream createWithChecksum(String filename) throws IOException {
    return create(filename, false);
  }

  /**
   * Creates a file and returns an open {@link FSDataOutputStream}.
   * <p>
   * If the file does not have a file system scheme, this uses the default FS.
   * <p>
   * This will neither produce checksum files nor overwrite a file that already
   * exists.
   *
   * @param filename The filename to create
   * @return An open FSDataOutputStream
   * @throws IOException if there is an error creating the file
   */
  public FSDataOutputStream createWithNoOverwrite(String filename) throws IOException {
    return create(filename, true, false);
  }

  private FSDataOutputStream create(String filename, boolean noChecksum) throws IOException {
    return create(filename, noChecksum, true);
  }

  private FSDataOutputStream create(String filename, boolean noChecksum, boolean overwrite) throws IOException {
    Path filePath = qualifiedPath(filename);
    // even though it was qualified using the default FS, it may not be in it
    FileSystem fs = filePath.getFileSystem(getConf());
    if (noChecksum && fs instanceof ChecksumFileSystem) {
      fs = ((ChecksumFileSystem) fs).getRawFileSystem();
    }
    return fs.create(filePath, overwrite);
  }

  /**
   * Returns a qualified {@link Path} for the {@code filename}.
   * <p>
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * @param filename The filename to qualify
   * @return A qualified Path for the filename
   * @throws IOException if there is an error creating a qualified path
   */
  public Path qualifiedPath(String filename) throws IOException {
    Path cwd = defaultFS().makeQualified(new Path("."));
    return new Path(filename).makeQualified(defaultFS().getUri(), cwd);
  }

  /**
   * Returns a {@link URI} for the {@code filename} that is a qualified Path or
   * a resource URI.
   * <p>
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * @param filename The filename to qualify
   * @return A qualified URI for the filename
   * @throws IOException if there is an error creating a qualified URI
   */
  public URI qualifiedURI(String filename) throws IOException {
    try {
      URI fileURI = new URI(filename);
      if (RESOURCE_URI_SCHEME.equals(fileURI.getScheme())) {
        return fileURI;
      }
    } catch (URISyntaxException ignore) {
    }
    return qualifiedPath(filename).toUri();
  }

  /**
   * Opens an existing file or resource.
   * <p>
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * @param filename The filename to open.
   * @return An open InputStream with the file contents
   * @throws IOException              if there is an error opening the file
   * @throws IllegalArgumentException If the file does not exist
   */
  public InputStream open(String filename) throws IOException {
    if (STDIN_AS_SOURCE.equals(filename)) {
      return System.in;
    }

    URI uri = qualifiedURI(filename);
    if (RESOURCE_URI_SCHEME.equals(uri.getScheme())) {
      return Resources.getResource(uri.getRawSchemeSpecificPart()).openStream();
    } else {
      Path filePath = new Path(uri);
      // even though it was qualified using the default FS, it may not be in it
      FileSystem fs = filePath.getFileSystem(getConf());
      return fs.open(filePath);
    }
  }

  public SeekableInput openSeekable(String filename) throws IOException {
    Path path = qualifiedPath(filename);
    // even though it was qualified using the default FS, it may not be in it
    FileSystem fs = path.getFileSystem(getConf());
    return new SeekableFSDataInputStream(fs, path);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    HadoopFileSystemURLStreamHandler.setDefaultConf(conf);
  }

  @Override
  public Configuration getConf() {
    // In case conf is null, we'll return an empty configuration
    // this can be on a local development machine
    return null != conf ? conf : createDefaultConf();
  }

  private Configuration createDefaultConf() {
    Configuration conf = new Configuration();
    // Support reading INT96 by default
    conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
    return conf;
  }

  /**
   * Returns a {@link ClassLoader} for a set of jars and directories.
   *
   * @param jars  A list of jar paths
   * @param paths A list of directories containing .class files
   * @return a classloader for the jars and paths
   * @throws MalformedURLException if  a jar or path is invalid
   */
  protected static ClassLoader loaderFor(List<String> jars, List<String> paths) throws MalformedURLException {
    return AccessController.doPrivileged(new GetClassLoader(urls(jars, paths)));
  }

  /**
   * Returns a {@link ClassLoader} for a set of jars.
   *
   * @param jars A list of jar paths
   * @return a classloader for the jars
   * @throws MalformedURLException if a URL is invalid
   */
  protected static ClassLoader loaderForJars(List<String> jars) throws MalformedURLException {
    return AccessController.doPrivileged(new GetClassLoader(urls(jars, null)));
  }

  /**
   * Returns a {@link ClassLoader} for a set of directories.
   *
   * @param paths A list of directories containing .class files
   * @return a classloader for the paths
   * @throws MalformedURLException if a path is invalid
   */
  protected static ClassLoader loaderForPaths(List<String> paths) throws MalformedURLException {
    return AccessController.doPrivileged(new GetClassLoader(urls(null, paths)));
  }

  private static List<URL> urls(List<String> jars, List<String> dirs) throws MalformedURLException {
    // check the additional jars and lib directories in the local FS
    final List<URL> urls = Lists.newArrayList();
    if (dirs != null) {
      for (String lib : dirs) {
        // final URLs must end in '/' for URLClassLoader
        File path = lib.endsWith("/") ? new File(lib) : new File(lib + "/");
        Preconditions.checkArgument(path.exists(), "Lib directory does not exist: %s", lib);
        Preconditions.checkArgument(path.isDirectory(), "Not a directory: %s", lib);
        Preconditions.checkArgument(
            path.canRead() && path.canExecute(),
            "Insufficient permissions to access lib directory: %s",
            lib);
        urls.add(path.toURI().toURL());
      }
    }
    if (jars != null) {
      for (String jar : jars) {
        File path = new File(jar);
        Preconditions.checkArgument(path.exists(), "Jar files does not exist: %s", jar);
        Preconditions.checkArgument(path.isFile(), "Not a file: %s", jar);
        Preconditions.checkArgument(path.canRead(), "Cannot read jar file: %s", jar);
        urls.add(path.toURI().toURL());
      }
    }
    return urls;
  }

  protected ParquetFileReader createParquetFileReader(String source) throws IOException {
    InputFile in = HadoopInputFile.fromPath(qualifiedPath(source), getConf());

    HadoopReadOptions.Builder optionsBuilder = HadoopReadOptions.builder(getConf());
    FileDecryptionProperties decryptionProperties = createFileDecryptionProperties();
    if (decryptionProperties != null) {
      optionsBuilder.withDecryption(decryptionProperties);
    }

    return ParquetFileReader.open(in, optionsBuilder.build());
  }

  protected FileDecryptionProperties createFileDecryptionProperties() {
    Configuration conf = getConf();
    String footerKeyHex = conf.get("parquet.encryption.footer.key");
    String columnKeysHex = conf.get("parquet.encryption.column.keys");

    if (footerKeyHex == null && columnKeysHex == null) {
      return null;
    }

    ConfigurableKeyRetriever keyRetriever = new ConfigurableKeyRetriever();
    FileDecryptionProperties.Builder builder =
        FileDecryptionProperties.builder().withPlaintextFilesAllowed();

    byte[] footerKey = hexToBytes(footerKeyHex);
    builder.withFooterKey(footerKey);

    parseAndSetColumnKeys(columnKeysHex, keyRetriever);
    builder.withKeyRetriever(keyRetriever);

    return builder.build();
  }

  private void parseAndSetColumnKeys(String columnKeysStr, ConfigurableKeyRetriever keyRetriever) {
    String[] keyToColumns = columnKeysStr.split(";");
    for (int i = 0; i < keyToColumns.length; ++i) {
      final String curKeyToColumns = keyToColumns[i].trim();
      if (curKeyToColumns.isEmpty()) {
        continue;
      }

      String[] parts = curKeyToColumns.split(":");
      if (parts.length != 2) {
        console.warn(
            "Incorrect key to columns mapping in parquet.encryption.column.keys: [{}]", curKeyToColumns);
        continue;
      }

      String columnKeyId = parts[0].trim();
      String columnNamesStr = parts[1].trim();
      String[] columnNames = columnNamesStr.split(",");

      byte[] columnKeyBytes = hexToBytes(columnKeyId);

      for (int j = 0; j < columnNames.length; ++j) {
        final String columnName = columnNames[j].trim();
        keyRetriever.putKey(columnName, columnKeyBytes);
        console.debug("Added decryption key for column: {}", columnName);
      }
    }
  }

  private byte[] hexToBytes(String hex) {

    if (hex.startsWith("0x") || hex.startsWith("0X")) {
      hex = hex.substring(2);
    }

    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }

  private static class ConfigurableKeyRetriever implements DecryptionKeyRetriever {
    private final Map<String, byte[]> keyMap = new java.util.HashMap<>();

    public void putKey(String keyId, byte[] keyBytes) {
      keyMap.put(keyId, keyBytes);
    }

    @Override
    public byte[] getKey(byte[] keyMetaData) {
      String keyId = new String(keyMetaData, StandardCharsets.UTF_8);
      return keyMap.get(keyId);
    }
  }

  protected <D> Iterable<D> openDataFile(final String source, Schema projection) throws IOException {
    Formats.Format format = Formats.detectFormat(open(source));
    switch (format) {
      case PARQUET:
        boolean isProtobufStyle = isProtobufStyleSchema(source);
        boolean useGroupReader = getConf().getBoolean(PARQUET_CLI_ENABLE_GROUP_READER, false);
        if (isProtobufStyle || useGroupReader) {
          final ParquetReader<Group> grp = ParquetReader.<Group>builder(
                  new GroupReadSupport(), qualifiedPath(source))
              .withConf(getConf())
              .build();
          return (Iterable<D>) asIterable(grp);
        }
        Configuration conf = new Configuration(getConf());
        // TODO: add these to the reader builder
        AvroReadSupport.setRequestedProjection(conf, projection);
        AvroReadSupport.setAvroReadSchema(conf, projection);
        final ParquetReader<D> parquet = AvroParquetReader.<D>builder(qualifiedPath(source))
            .disableCompatibility()
            .withDataModel(GenericData.get())
            .withConf(conf)
            .build();
        return new Iterable<D>() {
          @Override
          public Iterator<D> iterator() {
            return new Iterator<D>() {
              private boolean hasNext = false;
              private D next = advance();

              @Override
              public boolean hasNext() {
                return hasNext;
              }

              @Override
              public D next() {
                if (!hasNext) {
                  throw new NoSuchElementException();
                }
                D toReturn = next;
                this.next = advance();
                return toReturn;
              }

              private D advance() {
                try {
                  D next = parquet.read();
                  this.hasNext = (next != null);
                  return next;
                } catch (IOException e) {
                  throw new RuntimeException("Failed while reading Parquet file: " + source, e);
                }
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException("Remove is not supported");
              }
            };
          }
        };

      case AVRO:
        Iterable<D> avroReader = (Iterable<D>)
            DataFileReader.openReader(openSeekable(source), new GenericDatumReader<>(projection));
        return avroReader;

      default:
        if (source.endsWith("json")) {
          return new AvroJsonReader<>(open(source), projection);
        } else {
          Preconditions.checkArgument(projection == null, "Cannot select columns from text files");
          Iterable text = CharStreams.readLines(new InputStreamReader(open(source)));
          return text;
        }
    }
  }

  protected Schema getAvroSchema(String source) throws IOException {
    Formats.Format format;
    try (SeekableInput in = openSeekable(source)) {
      format = Formats.detectFormat((InputStream) in);
      in.seek(0);

      switch (format) {
        case PARQUET:
          return Schemas.fromParquet(getConf(), qualifiedURI(source));
        case AVRO:
          return Schemas.fromAvro(open(source));
        case TEXT:
          if (source.endsWith("avsc")) {
            return Schemas.fromAvsc(open(source));
          } else if (source.endsWith("json")) {
            return Schemas.fromJSON("json", open(source));
          }
        default:
      }

      throw new IllegalArgumentException(String.format("Could not determine file format of %s.", source));
    }
  }
}
