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
package org.apache.parquet.tools.command;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPage.Visitor;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.tools.util.PrettyPrintWriter;
import org.apache.parquet.tools.util.PrettyPrintWriter.WhiteSpaceHandler;

import com.google.common.base.Joiner;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class DumpCommand extends ArgsOnlyCommand {
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final CharsetDecoder UTF8_DECODER = UTF8.newDecoder();

    public static final String TABS = "    ";
    public static final int BLOCK_BUFFER_SIZE = 64 * 1024;
    public static final String[] USAGE = new String[] { "<input>", "where <input> is the parquet file to print to stdout" };

    private static CRC32 crc = new CRC32();

    public static final Options OPTIONS;
    static {
        OPTIONS = new Options();
        Option md = OptionBuilder.withLongOpt("disable-meta")
                                 .withDescription("Do not dump row group and page metadata")
                                 .create('m');

        Option dt = OptionBuilder.withLongOpt("disable-data")
                                 .withDescription("Do not dump column data")
                                 .create('d');

        Option nocrop = OptionBuilder.withLongOpt("disable-crop")
                                 .withDescription("Do not crop the output based on console width")
                                 .create('n');

        Option cl = OptionBuilder.withLongOpt("column")
                                 .withDescription("Dump only the given column, can be specified more than once")
                                 .hasArg()
                                 .create('c');

        OPTIONS.addOption(md);
        OPTIONS.addOption(dt);
        OPTIONS.addOption(nocrop);
        OPTIONS.addOption(cl);
    }

    public DumpCommand() {
        super(1, 1);
    }

    @Override
    public Options getOptions() {
        return OPTIONS;
    }

    @Override
    public String[] getUsageDescription() {
        return USAGE;
    }

  @Override
  public String getCommandDescription() {
    return "Prints the content and metadata of a Parquet file";
  }

  @Override
    public void execute(CommandLine options) throws Exception {
        super.execute(options);

        String[] args = options.getArgs();
        String input = args[0];

        Configuration conf = new Configuration();
        Path inpath = new Path(input);

        ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inpath, NO_FILTER);
        MessageType schema = metaData.getFileMetaData().getSchema();

        boolean showmd = !options.hasOption('m');
        boolean showdt = !options.hasOption('d');
        boolean cropoutput = !options.hasOption('n');

        Set<String> showColumns = null;
        if (options.hasOption('c')) {
            String[] cols = options.getOptionValues('c');
            showColumns = new HashSet<String>(Arrays.asList(cols));
        }

        PrettyPrintWriter out = prettyPrintWriter(cropoutput);
        dump(out, metaData, schema, inpath, showmd, showdt, showColumns);
    }

    public static void dump(PrettyPrintWriter out, ParquetMetadata meta, MessageType schema, Path inpath, boolean showmd, boolean showdt, Set<String> showColumns) throws IOException {
        Configuration conf = new Configuration();

        List<BlockMetaData> blocks = meta.getBlocks();
        List<ColumnDescriptor> columns = schema.getColumns();
        if (showColumns != null) {
            columns = new ArrayList<ColumnDescriptor>();
            for (ColumnDescriptor column : schema.getColumns()) {
                String path = Joiner.on('.').skipNulls().join(column.getPath());
                if (showColumns.contains(path)) {
                    columns.add(column);
                }
            }
        }

        if (showmd) {
          long group = 0;
          for (BlockMetaData block : blocks) {
            if (group != 0)
              out.println();
            out.format("row group %d%n", group++);
            out.rule('-');

            List<ColumnChunkMetaData> ccmds = block.getColumns();
            if (showColumns != null) {
              ccmds = new ArrayList<ColumnChunkMetaData>();
              for (ColumnChunkMetaData ccmd : block.getColumns()) {
                String path = Joiner.on('.').skipNulls().join(ccmd.getPath().toArray());
                if (showColumns.contains(path)) {
                  ccmds.add(ccmd);
                }
              }
            }

            MetadataUtils.showDetails(out, ccmds);

            List<BlockMetaData> rblocks = Collections.singletonList(block);
            try (ParquetFileReader freader = new ParquetFileReader(conf, meta.getFileMetaData(), inpath, rblocks,
              columns)) {
              PageReadStore store = freader.readNextRowGroup();
              while (store != null) {
                out.incrementTabLevel();
                for (ColumnDescriptor column : columns) {
                  out.println();
                  dump(out, store, column);
                }
                out.decrementTabLevel();

                store = freader.readNextRowGroup();
              }
              out.flushColumns();
            }
          }
        }

        if (showdt) {
            boolean first = true;
            for (ColumnDescriptor column : columns) {
                if (!first || showmd) out.println();
                first = false;

                out.format("%s %s%n", column.getType(), Joiner.on('.').skipNulls().join(column.getPath()));
                out.rule('-');
                try {
                    long page = 1;
                    long total = blocks.size();
                    long offset = 1;
                    try(ParquetFileReader freader = new ParquetFileReader(
                      conf, meta.getFileMetaData(), inpath, blocks, Collections.singletonList(column))){
                      PageReadStore store = freader.readNextRowGroup();
                      while (store != null) {
                        ColumnReadStoreImpl crstore = new ColumnReadStoreImpl(
                          store, new DumpGroupConverter(), schema,
                          meta.getFileMetaData().getCreatedBy());
                        dump(out, crstore, column, page++, total, offset);

                        offset += store.getRowCount();
                        store = freader.readNextRowGroup();
                      }
                    }
                    out.flushColumns();
                } finally {
                    out.flushColumns();
                }
            }
        }
    }

    private static boolean verifyCrc(int referenceCrc, byte[] bytes) {
      crc.reset();
      crc.update(bytes);
      return crc.getValue() == ((long) referenceCrc & 0xffffffffL);
    }

    public static void dump(final PrettyPrintWriter out, PageReadStore store, ColumnDescriptor column) throws IOException {
        PageReader reader = store.getPageReader(column);

        long vc = reader.getTotalValueCount();
        int rmax = column.getMaxRepetitionLevel();
        int dmax = column.getMaxDefinitionLevel();
        out.format("%s TV=%d RL=%d DL=%d", Joiner.on('.').skipNulls().join(column.getPath()), vc, rmax, dmax);

        DictionaryPage dict = reader.readDictionaryPage();
        if (dict != null) {
            out.format(" DS:%d", dict.getDictionarySize());
            out.format(" DE:%s", dict.getEncoding());
        }

        out.println();
        out.rule('-');

        DataPage page = reader.readPage();
        for (long count = 0; page != null; count++) {
            out.format("page %d:", count);
            page.accept(new Visitor<Void>() {
              @Override
              public Void visit(DataPageV1 pageV1) {
                out.format(" DLE:%s", pageV1.getDlEncoding());
                out.format(" RLE:%s", pageV1.getRlEncoding());
                out.format(" VLE:%s", pageV1.getValueEncoding());
                Statistics<?> statistics = pageV1.getStatistics();
                if (statistics != null) {
                  out.format(" ST:[%s]", statistics);
                } else {
                  out.format(" ST:[none]");
                }
                if (pageV1.getCrc().isPresent()) {
                  try {
                    out.format(" CRC:%s", verifyCrc(pageV1.getCrc().getAsInt(), pageV1.getBytes().toByteArray()) ? "[verified]" : "[PAGE CORRUPT]");
                  } catch (IOException e) {
                    out.format(" CRC:[error getting page bytes]");
                  }
                } else {
                  out.format(" CRC:[none]");
                }
                return null;
              }

              @Override
              public Void visit(DataPageV2 pageV2) {
                out.format(" DLE:RLE");
                out.format(" RLE:RLE");
                out.format(" VLE:%s", pageV2.getDataEncoding());
                Statistics<?> statistics = pageV2.getStatistics();
                if (statistics != null) {
                  out.format(" ST:[%s]", statistics);
                } else {
                  out.format(" ST:[none]");
                }
                return null;
              }
            });
            out.format(" SZ:%d", page.getUncompressedSize());
            out.format(" VC:%d", page.getValueCount());
            out.println();
            page = reader.readPage();
        }
    }

    public static void dump(PrettyPrintWriter out, ColumnReadStoreImpl crstore, ColumnDescriptor column, long page, long total, long offset) throws IOException {
        int dmax = column.getMaxDefinitionLevel();
        ColumnReader creader = crstore.getColumnReader(column);
        out.format("*** row group %d of %d, values %d to %d ***%n", page, total, offset, offset + creader.getTotalValueCount() - 1);

        for (long i = 0, e = creader.getTotalValueCount(); i < e; ++i) {
            int rlvl = creader.getCurrentRepetitionLevel();
            int dlvl = creader.getCurrentDefinitionLevel();

            out.format("value %d: R:%d D:%d V:", offset+i, rlvl, dlvl);
            if (dlvl == dmax) {
              PrimitiveStringifier stringifier =  column.getPrimitiveType().stringifier();
              switch (column.getType()) {
                case FIXED_LEN_BYTE_ARRAY:
                case INT96:
                case BINARY:
                  out.print(stringifier.stringify(creader.getBinary()));
                  break;
                case BOOLEAN:
                  out.print(stringifier.stringify(creader.getBoolean()));
                  break;
                case DOUBLE:
                  out.print(stringifier.stringify(creader.getDouble()));
                  break;
                case FLOAT:
                  out.print(stringifier.stringify(creader.getFloat()));
                  break;
                case INT32:
                  out.print(stringifier.stringify(creader.getInteger()));
                  break;
                case INT64:
                  out.print(stringifier.stringify(creader.getLong()));
                  break;
              }
            } else {
                out.format("<null>");
            }

            out.println();
            creader.consume();
        }
    }

    public static String binaryToString(Binary value) {
        byte[] data = value.getBytesUnsafe();
        if (data == null) return null;

        try {
            CharBuffer buffer = UTF8_DECODER.decode(value.toByteBuffer());
            return buffer.toString();
        } catch (Exception ex) {
        }

        return "<bytes...>";
    }

    public static BigInteger binaryToBigInteger(Binary value) {
        byte[] data = value.getBytesUnsafe();
        if (data == null) return null;

        return new BigInteger(data);
    }

    private static PrettyPrintWriter prettyPrintWriter(boolean cropOutput) {
        PrettyPrintWriter.Builder builder = PrettyPrintWriter.stdoutPrettyPrinter()
                .withAutoColumn()
                .withWhitespaceHandler(WhiteSpaceHandler.ELIMINATE_NEWLINES)
                .withColumnPadding(1)
                .withMaxBufferedLines(1000000)
                .withFlushOnTab();

        if (cropOutput) {
            builder.withAutoCrop();
        }

        return builder.build();
    }

    private static final class DumpGroupConverter extends GroupConverter {
        @Override public void start() { }
        @Override public void end() { }
        @Override public Converter getConverter(int fieldIndex) { return new DumpConverter(); }
    }

    private static final class DumpConverter extends PrimitiveConverter {
        @Override public GroupConverter asGroupConverter() { return new DumpGroupConverter(); }
    }
}
