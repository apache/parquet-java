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
package parquet.tools.command;

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.impl.ColumnReadStoreImpl;
import parquet.column.page.DataPage;
import parquet.column.page.DataPage.Visitor;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.MessageType;
import parquet.tools.util.MetadataUtils;
import parquet.tools.util.PrettyPrintWriter;
import parquet.tools.util.PrettyPrintWriter.WhiteSpaceHandler;

import com.google.common.base.Joiner;

public class DumpCommand extends ArgsOnlyCommand {
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final CharsetDecoder UTF8_DECODER = UTF8.newDecoder();

    public static final String TABS = "    ";
    public static final int BLOCK_BUFFER_SIZE = 64 * 1024;
    public static final String[] USAGE = new String[] { "<input>", "where <input> is the parquet file to print to stdout" };

    public static final Options OPTIONS;
    static {
        OPTIONS = new Options();
        Option md = OptionBuilder.withLongOpt("disable-meta")
                                 .withDescription("Do not dump row group and page metadata")
                                 .create('m');

        Option dt = OptionBuilder.withLongOpt("disable-data")
                                 .withDescription("Do not dump column data")
                                 .create('d');

        Option cl = OptionBuilder.withLongOpt("column")
                                 .withDescription("Dump only the given column, can be specified more than once")
                                 .hasArgs()
                                 .create('c');

        OPTIONS.addOption(md);
        OPTIONS.addOption(dt);
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
    public void execute(CommandLine options) throws Exception {
        super.execute(options);

        String[] args = options.getArgs();
        String input = args[0];

        Configuration conf = new Configuration();
        Path inpath = new Path(input);

        ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inpath);
        MessageType schema = metaData.getFileMetaData().getSchema();

        PrettyPrintWriter out = PrettyPrintWriter.stdoutPrettyPrinter()
                                                 .withAutoColumn()
                                                 .withAutoCrop()
                                                 .withWhitespaceHandler(WhiteSpaceHandler.ELIMINATE_NEWLINES)
                                                 .withColumnPadding(1)
                                                 .withMaxBufferedLines(1000000)
                                                 .withFlushOnTab()
                                                 .build();

        boolean showmd = !options.hasOption('m');
        boolean showdt = !options.hasOption('d');

        Set<String> showColumns = null;
        if (options.hasOption('c')) {
            String[] cols = options.getOptionValues('c');
            showColumns = new HashSet<String>(Arrays.asList(cols));
        }

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

        ParquetFileReader freader = null;
        if (showmd) {
            try {
                long group = 0;
                for (BlockMetaData block : blocks) {
                    if (group != 0) out.println();
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
                    freader = new ParquetFileReader(conf, inpath, rblocks, columns);
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
            } finally {
                if (freader != null) {
                    freader.close();
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
                    freader = new ParquetFileReader(conf, inpath, blocks, Collections.singletonList(column));
                    PageReadStore store = freader.readNextRowGroup();
                    while (store != null) {
                        ColumnReadStoreImpl crstore = new ColumnReadStoreImpl(store, new DumpGroupConverter(), schema);
                        dump(out, crstore, column, page++, total, offset);

                        offset += store.getRowCount();
                        store = freader.readNextRowGroup();
                    }

                    out.flushColumns();
                } finally {
                    out.flushColumns();
                    if (freader != null) {
                        freader.close();
                    }
                }
            }
        }
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
                return null;
              }

              @Override
              public Void visit(DataPageV2 pageV2) {
                out.format(" DLE:RLE");
                out.format(" RLE:RLE");
                out.format(" VLE:%s", pageV2.getDataEncoding());
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
                switch (column.getType()) {
                case BINARY:  out.format("%s", binaryToString(creader.getBinary())); break;
                case BOOLEAN: out.format("%s", creader.getBoolean()); break;
                case DOUBLE:  out.format("%s", creader.getDouble()); break;
                case FLOAT:   out.format("%s", creader.getFloat()); break;
                case INT32:   out.format("%s", creader.getInteger()); break;
                case INT64:   out.format("%s", creader.getLong()); break;
                case INT96:   out.format("%s", binaryToBigInteger(creader.getBinary())); break;
                case FIXED_LEN_BYTE_ARRAY: out.format("%s", binaryToString(creader.getBinary())); break;
                }
            } else {
                out.format("<null>");
            }

            out.println();
            creader.consume();
        }
    }

    public static String binaryToString(Binary value) {
        byte[] data = value.getBytes();
        if (data == null) return null;

        try {
            CharBuffer buffer = UTF8_DECODER.decode(value.toByteBuffer());
            return buffer.toString();
        } catch (Throwable th) {
        }

        return "<bytes...>";
    }

    public static BigInteger binaryToBigInteger(Binary value) {
        byte[] data = value.getBytes();
        if (data == null) return null;

        return new BigInteger(data);
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
