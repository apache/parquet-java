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
package redelm.hadoop;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import redelm.hadoop.metadata.BlockMetaData;
import redelm.hadoop.metadata.ColumnChunkMetaData;
import redelm.hadoop.metadata.RedelmMetaData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils;

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
    List<FileStatus> statuses;
    if (fileStatus.isDir()) {
      System.out.println("listing files in " + fileStatus.getPath());
      statuses = Arrays.asList(fs.listStatus(fileStatus.getPath(), new Utils.OutputFileUtils.OutputFilesFilter()));
    } else {
      statuses = new ArrayList<FileStatus>();
      statuses.add(fileStatus);
    }
    int i = 0;
    ExecutorService threadPool = Executors.newFixedThreadPool(5);
    try {

      List<Future<RedelmMetaData>> footers = new ArrayList<Future<RedelmMetaData>>();
      for (final FileStatus currentFile : statuses) {
        footers.add(threadPool.submit(new Callable<RedelmMetaData>() {
          @Override
          public RedelmMetaData call() throws Exception {
            RedelmMetaData footer = RedelmFileReader.readFooter(configuration, currentFile);
            return footer;
          }
        }));
      }
      int blockCount = 0;
      for (Future<RedelmMetaData> futureFooter : footers) {
        RedelmMetaData footer = futureFooter.get();
        System.out.println("Reading footers: " + (++i * 100 / statuses.size()) + "%");
        //  System.out.println(Footer.toPrettyJSON(footer));
        List<BlockMetaData> blocks = footer.getBlocks();
        for (BlockMetaData blockMetaData : blocks) {
          ++ blockCount;
          List<ColumnChunkMetaData> columns = blockMetaData.getColumns();
          for (ColumnChunkMetaData columnMetaData : columns) {
            add(
                columnMetaData.getPath(),
                columnMetaData.getValueCount(),
                columnMetaData.getTotalSize(),
                columnMetaData.getTotalUncompressedSize());
          }
        }
      }

      Set<Entry<String, ColStats>> entries = stats.entrySet();
      long total = 0;
      long totalValues = 0;
      long totalUnc = 0;
      for (Entry<String, ColStats> entry : entries) {
        ColStats colStats = entry.getValue();
        total += colStats.allStats.total;
        totalValues += colStats.valueCountStats.total;
        totalUnc += colStats.uncStats.total;
      }

//      for (Entry<String, ColStats> entry : entries) {
//        ColStats colStats = entry.getValue();
//        System.out.println(entry.getKey() +" " + percent(colStats.allStats.total, totalData) + "% of data " + colStats);
//      }

//      printTotalString("repetition levels", totalRL, totalRL);
//      printTotalString("definition levels", totalDL, totalDL);
//      printTotalString("data", totalData, totalDataUnc);
//      long totalUnc = totalRL + totalDL + totalDataUnc;
//      System.out.println("Repetition and definition levels overhead: " + percent(totalRL + totalDL,totalData) + "% of data size");
      System.out.println("average block size: "+ humanReadable(total/blockCount)+" (raw "+humanReadable(totalUnc/blockCount)+")");
      System.out.println("average value count: "+ humanReadable(totalValues/blockCount));
    } finally {
      threadPool.shutdownNow();
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
      return size + "B";
    }
    long currentSize = size;
    long previousSize = 0;
    int count = 0;
    String[] unit = {"B", "KB", "MB", "GB", "TB", "PB"};
    while (currentSize >= 1000) {
      previousSize = currentSize;
      currentSize = currentSize / 1000;
      ++ count;
    }
    return ((float)previousSize/1000) + unit[count];
  }

  private static Map<String, ColStats> stats = new HashMap<String, ColStats>();

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
    int blocks = 0;

    public void add(long valueCount, long size, long uncSize) {
      ++blocks;
      valueCountStats.add(valueCount);
      allStats.add(size);
      uncStats.add(uncSize);
    }

    @Override
    public String toString() {
      long raw = uncStats.total;
      long compressed = allStats.total;
      return allStats.toString(blocks) + " (raw data: " + humanReadable(raw) + (raw == 0 ? "" : " saving " + (raw - compressed)*100/raw + "%") + ")\n"
      + "  values: "+valueCountStats.toString(blocks) + "\n"
      + "  uncompressed: "+uncStats.toString(blocks);
    }

  }

  private static void add(String[] path, long valueCount, long size, long uncSize) {
    String key = Arrays.toString(path);
    ColStats colStats = stats.get(key);
    if (colStats == null) {
      colStats = new ColStats();
      stats.put(key, colStats);
    }
    colStats.add(valueCount, size, uncSize);
  }
}
