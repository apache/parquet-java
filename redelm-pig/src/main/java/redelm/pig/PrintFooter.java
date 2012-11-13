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
package redelm.pig;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils;

public class PrintFooter {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("usage PrintFooter <path>");
      return;
    }
    Path path = new Path(new URI(args[0]));
    final FileSystem fs = path.getFileSystem(new Configuration());
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

      List<Future<Footer>> footers = new ArrayList<Future<Footer>>();
      for (final FileStatus currentFile : statuses) {
        footers.add(threadPool.submit(new Callable<Footer>() {
          @Override
          public Footer call() throws Exception {
            Footer footer = Footer.fromMetaDataBlocks(RedelmFileReader.readFooter(fs.open(currentFile.getPath()), currentFile.getLen()));
            return footer;
          }
        }));
      }
      int blockCount = 0;
      for (Future<Footer> futureFooter : footers) {
        Footer footer = futureFooter.get();
        System.out.println("Reading footers: " + (++i * 100 / statuses.size()) + "%");
        //  System.out.println(Footer.toPrettyJSON(footer));
        List<BlockMetaData> blocks = footer.getBlocks();
        for (BlockMetaData blockMetaData : blocks) {
          ++ blockCount;
          List<ColumnMetaData> columns = blockMetaData.getColumns();
          for (ColumnMetaData columnMetaData : columns) {
            add(
                columnMetaData.getPath(),
                columnMetaData.getColumnLength(),
                columnMetaData.getRepetitionLength(),
                columnMetaData.getRepetitionUncompressedLength(),
                columnMetaData.getDefinitionLength(),
                columnMetaData.getDefinitionUncompressedLength(),
                columnMetaData.getDataLength(),
                columnMetaData.getDataUncompressedLength());
          }
        }
      }

      Set<Entry<String, ColStats>> entries = stats.entrySet();
      long total = 0;
      long totalRL = 0;
      long totalRLUnc = 0;
      long totalDL = 0;
      long totalDLUnc = 0;
      long totalData = 0;
      long totalDataUnc = 0;
      for (Entry<String, ColStats> entry : entries) {
        ColStats colStats = entry.getValue();
        total += colStats.allStats.total;
        totalRL += colStats.repStats.total;
        totalRLUnc += colStats.repUncStats.total;
        totalDL += colStats.defStats.total;
        totalDLUnc += colStats.defUncStats.total;
        totalData += colStats.dataStats.total;
        totalDataUnc += colStats.dataUncStats.total;
      }

      for (Entry<String, ColStats> entry : entries) {
        ColStats colStats = entry.getValue();
        System.out.println(entry.getKey() +" " + percent(colStats.dataStats.total, totalData) + "% of data " + colStats);
      }

      printTotalString("repetition levels", totalRL, totalRLUnc);
      printTotalString("definition levels", totalDL, totalDLUnc);
      printTotalString("data", totalData, totalDataUnc);
      long totalUnc = totalRLUnc + totalDLUnc + totalDataUnc;
      System.out.println("Repetition and definition levels overhead: " + percent(totalRL + totalDL,totalData) + "% of data size");
      System.out.println("average block size: "+ humanReadable(total/blockCount)+" (raw "+humanReadable(totalUnc/blockCount)+")");
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

    Stats allStats = new Stats();
    Stats repStats = new Stats();
    Stats defStats = new Stats();
    Stats dataStats = new Stats();
    Stats repUncStats = new Stats();
    Stats defUncStats = new Stats();
    Stats dataUncStats = new Stats();
    int blocks = 0;

    public void add(long columnLength, long rep, long repUnc, long def, long defUnc, long data, long dataUnc) {
      ++blocks;
      allStats.add(columnLength);
      repStats.add(rep);
      repUncStats.add(repUnc);
      defStats.add(def);
      defUncStats.add(defUnc);
      dataStats.add(data);
      dataUncStats.add(dataUnc);
    }

    @Override
    public String toString() {
      long raw = dataUncStats.total;
      long compressed = allStats.total;
      return allStats.toString(blocks) + " (raw data: " + humanReadable(raw) + (raw == 0 ? "" : " saving " + (raw - compressed)*100/raw + "%") + ")\n"
      + "  r: "+repStats.toString(blocks) + " (uncompressed "+repUncStats.toString(blocks)+") \n"
      + "  d: "+defStats.toString(blocks) + " (uncompressed "+defUncStats.toString(blocks)+")\n"
      + "  data: "+dataStats.toString(blocks)+" (uncompressed "+dataUncStats.toString(blocks)+")";
    }

  }

  private static void add(String[] path, long columnLength, long rep, long repUnc, long def, long defUnc, long data, long dataUnc) {
    String key = Arrays.toString(path);
    ColStats colStats = stats.get(key);
    if (colStats == null) {
      colStats = new ColStats();
      stats.put(key, colStats);
    }
    colStats.add(columnLength, rep, repUnc, def, defUnc, data, dataUnc);
  }
}
