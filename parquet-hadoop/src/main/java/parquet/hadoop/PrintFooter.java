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
package parquet.hadoop;

import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static parquet.hadoop.ParquetFileWriter.PARQUET_METADATA_FILE;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.statistics.Statistics;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ParquetDecodingException;
import parquet.schema.MessageType;

/**
 * Utility to print footer information
 * @author Julien Le Dem
 *
 */
public class PrintFooter {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("usage PrintFooter <path>");
      return;
    }
    Path path = new Path(new URI(args[0]));
    final Configuration configuration = new Configuration();

    final FileSystem fs = path.getFileSystem(configuration);
    FileStatus fileStatus = fs.getFileStatus(path);
    Path summary = new Path(fileStatus.getPath(), PARQUET_METADATA_FILE);
    if (fileStatus.isDir() && fs.exists(summary)) {
      System.out.println("reading summary file");
      FileStatus summaryStatus = fs.getFileStatus(summary);
      List<Footer> readSummaryFile = ParquetFileReader.readSummaryFile(configuration, summaryStatus);
      for (Footer footer : readSummaryFile) {
        add(footer.getParquetMetadata());
      }
    } else {
      List<FileStatus> statuses;
      if (fileStatus.isDir()) {
        System.out.println("listing files in " + fileStatus.getPath());
        statuses = Arrays.asList(fs.listStatus(fileStatus.getPath(), new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return !path.getName().startsWith("_");
          }
        }));
      } else {
        statuses = new ArrayList<FileStatus>();
        statuses.add(fileStatus);
      }
      System.out.println("opening " + statuses.size() + " files");
      int i = 0;
      ExecutorService threadPool = Executors.newFixedThreadPool(5);
      try {
        long t0 = System.currentTimeMillis();
        Deque<Future<ParquetMetadata>> footers = new LinkedBlockingDeque<Future<ParquetMetadata>>();
        for (final FileStatus currentFile : statuses) {
          footers.add(threadPool.submit(new Callable<ParquetMetadata>() {
            @Override
            public ParquetMetadata call() throws Exception {
              try {
                ParquetMetadata footer = ParquetFileReader.readFooter(configuration, currentFile, NO_FILTER);
                return footer;
              } catch (Exception e) {
                throw new ParquetDecodingException("could not read footer", e);
              }
            }
          }));
        }
        int previousPercent = 0;
        int n = 60;
        System.out.print("0% [");
        for (int j = 0; j < n; j++) {
          System.out.print(" ");

        }
        System.out.print("] 100%");
        for (int j = 0; j < n + 6; j++) {
          System.out.print('\b');
        }
        while (!footers.isEmpty()) {
          Future<ParquetMetadata> futureFooter = footers.removeFirst();
          if (!futureFooter.isDone()) {
            footers.addLast(futureFooter);
            continue;
          }
          ParquetMetadata footer = futureFooter.get();
          int currentPercent = (++i * n / statuses.size());
          while (currentPercent > previousPercent) {
            System.out.print("*");
            previousPercent ++;
          }
          add(footer);
        }
        System.out.println("");
        long t1 = System.currentTimeMillis();
        System.out.println("read all footers in " + (t1 - t0) + " ms");
      } finally {
        threadPool.shutdownNow();
      }
    }
    Set<Entry<ColumnDescriptor, ColStats>> entries = stats.entrySet();
    long total = 0;
    long totalUnc = 0;
    for (Entry<ColumnDescriptor, ColStats> entry : entries) {
      ColStats colStats = entry.getValue();
      total += colStats.allStats.total;
      totalUnc += colStats.uncStats.total;
    }

    for (Entry<ColumnDescriptor, ColStats> entry : entries) {
      ColStats colStats = entry.getValue();
      System.out.println(entry.getKey() +" " + percent(colStats.allStats.total, total) + "% of all space " + colStats);
    }

    System.out.println("number of blocks: " + blockCount);
    System.out.println("total data size: " + humanReadable(total) + " (raw " + humanReadable(totalUnc) + ")");
    System.out.println("total record: " + humanReadable(recordCount));
    System.out.println("average block size: " + humanReadable(total/blockCount) + " (raw " + humanReadable(totalUnc/blockCount) + ")");
    System.out.println("average record count: " + humanReadable(recordCount/blockCount));
  }

  private static void add(ParquetMetadata footer) {
    for (BlockMetaData blockMetaData : footer.getBlocks()) {
      ++ blockCount;
      MessageType schema = footer.getFileMetaData().getSchema();
      recordCount += blockMetaData.getRowCount();
      List<ColumnChunkMetaData> columns = blockMetaData.getColumns();
      for (ColumnChunkMetaData columnMetaData : columns) {
        ColumnDescriptor desc = schema.getColumnDescription(columnMetaData.getPath().toArray());
        add(
            desc,
            columnMetaData.getValueCount(),
            columnMetaData.getTotalSize(),
            columnMetaData.getTotalUncompressedSize(),
            columnMetaData.getEncodings(),
            columnMetaData.getStatistics());
      }
    }
  }

  private static void printTotalString(String message, long total, long totalUnc) {
    System.out.println("total "+message+": " + humanReadable(total) + " (raw "+humanReadable(totalUnc)+" saved "+percentComp(totalUnc, total)+"%)");
  }

  private static float percentComp(long raw, long compressed) {
    return percent(raw - compressed, raw);
  }

  private static float percent(long numerator, long denominator) {
    return ((float)((numerator)*1000/denominator))/10;
  }

  private static String humanReadable(long size) {
    if (size < 1000) {
      return String.valueOf(size);
    }
    long currentSize = size;
    long previousSize = size * 1000;
    int count = 0;
    String[] unit = {"", "K", "M", "G", "T", "P"};
    while (currentSize >= 1000) {
      previousSize = currentSize;
      currentSize = currentSize / 1000;
      ++ count;
    }
    return ((float)previousSize/1000) + unit[count];
  }

  private static Map<ColumnDescriptor, ColStats> stats = new LinkedHashMap<ColumnDescriptor, ColStats>();
  private static int blockCount = 0;
  private static long recordCount = 0;

  private static class Stats {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long total = 0;

    public void add(long  length) {
      min = Math.min(length, min);
      max = Math.max(length, max);
      total += length;
    }

    public String toString(int blocks) {
      return
          "min: " + humanReadable(min) +
          " max: " + humanReadable(max) +
          " average: " + humanReadable(total/blocks) +
          " total: " + humanReadable(total);
    }
  }

  private static class ColStats {

    Stats valueCountStats = new Stats();
    Stats allStats = new Stats();
    Stats uncStats = new Stats();
    Set<Encoding> encodings = new TreeSet<Encoding>();
    Statistics colValuesStats = null;
    int blocks = 0;

    public void add(long valueCount, long size, long uncSize, Collection<Encoding> encodings, Statistics colValuesStats) {
      ++blocks;
      valueCountStats.add(valueCount);
      allStats.add(size);
      uncStats.add(uncSize);
      this.encodings.addAll(encodings);
      this.colValuesStats = colValuesStats;
    }

    @Override
    public String toString() {
      long raw = uncStats.total;
      long compressed = allStats.total;
      return encodings + " " + allStats.toString(blocks) + " (raw data: " + humanReadable(raw) + (raw == 0 ? "" : " saving " + (raw - compressed)*100/raw + "%") + ")\n"
      + "  values: "+valueCountStats.toString(blocks) + "\n"
      + "  uncompressed: "+uncStats.toString(blocks) + "\n"
      + "  column values statistics: " + colValuesStats.toString();
    }

  }

  private static void add(ColumnDescriptor desc, long valueCount, long size, long uncSize, Collection<Encoding> encodings, Statistics colValuesStats) {
    ColStats colStats = stats.get(desc);
    if (colStats == null) {
      colStats = new ColStats();
      stats.put(desc, colStats);
    }
    colStats.add(valueCount, size, uncSize, encodings, colValuesStats);
  }
}
