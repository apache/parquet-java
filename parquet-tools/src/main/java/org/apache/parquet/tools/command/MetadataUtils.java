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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.tools.util.PrettyPrintWriter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class MetadataUtils {
  static void showDetails(PrettyPrintWriter out, ParquetMetadata meta, boolean showOriginalTypes) {
    showDetails(out, meta.getFileMetaData(), showOriginalTypes);

    long i = 1;
    for (BlockMetaData bmeta : meta.getBlocks()) {
      out.println();
      showDetails(out, bmeta, i++);
    }
  }

  static void showDetails(PrettyPrintWriter out, FileMetaData meta, boolean showOriginalTypes) {
    out.format("creator: %s%n", meta.getCreatedBy());

    Map<String, String> extra = meta.getKeyValueMetaData();
    if (extra != null) {
      for (Map.Entry<String, String> entry : meta.getKeyValueMetaData().entrySet()) {
        out.print("extra: ");
        out.incrementTabLevel();
        out.format("%s = %s%n", entry.getKey(), entry.getValue());
        out.decrementTabLevel();
      }
    }

    out.println();
    out.format("file schema: %s%n", meta.getSchema().getName());
    out.rule('-');
    showDetails(out, meta.getSchema(), showOriginalTypes);
  }

  private static void showDetails(PrettyPrintWriter out, BlockMetaData meta, Long num) {
    long rows = meta.getRowCount();
    long tbs = meta.getTotalByteSize();
    long offset = meta.getStartingPos();

    out.format("row group%s: RC:%d TS:%d OFFSET:%d%n", (num == null ? "" : " " + num), rows, tbs, offset);
    out.rule('-');
    showDetails(out, meta.getColumns());
  }

  static void showDetails(PrettyPrintWriter out, List<ColumnChunkMetaData> ccmeta) {
    Map<String, Object> chunks = new LinkedHashMap<String, Object>();
    for (ColumnChunkMetaData cmeta : ccmeta) {
      String[] path = cmeta.getPath().toArray();

      Map<String, Object> current = chunks;
      for (int i = 0; i < path.length - 1; ++i) {
        String next = path[i];
        if (!current.containsKey(next)) {
          current.put(next, new LinkedHashMap<String, Object>());
        }

        current = (Map<String, Object>) current.get(next);
      }

      current.put(path[path.length - 1], cmeta);
    }

    showColumnChunkDetails(out, chunks, 0);
  }

  private static void showColumnChunkDetails(PrettyPrintWriter out, Map<String, Object> current, int depth) {
    for (Map.Entry<String, Object> entry : current.entrySet()) {
      String name = Strings.repeat(".", depth) + entry.getKey();
      Object value = entry.getValue();

      if (value instanceof Map) {
        out.println(name + ": ");
        showColumnChunkDetails(out, (Map<String, Object>) value, depth + 1);
      } else {
        out.print(name + ": ");
        showDetails(out, (ColumnChunkMetaData) value, false);
      }
    }
  }

  private static void showDetails(PrettyPrintWriter out, ColumnChunkMetaData meta, boolean name) {
    long doff = meta.getDictionaryPageOffset();
    long foff = meta.getFirstDataPageOffset();
    long tsize = meta.getTotalSize();
    long usize = meta.getTotalUncompressedSize();
    long count = meta.getValueCount();
    double ratio = usize / (double) tsize;
    String encodings = Joiner.on(',').skipNulls().join(meta.getEncodings());

    if (name) {
      String path = Joiner.on('.').skipNulls().join(meta.getPath());
      out.format("%s: ", path);
    }

    out.format(" %s", meta.getType());
    out.format(" %s", meta.getCodec());
    out.format(" DO:%d", doff);
    out.format(" FPO:%d", foff);
    out.format(" SZ:%d/%d/%.2f", tsize, usize, ratio);
    out.format(" VC:%d", count);
    if (!encodings.isEmpty())
      out.format(" ENC:%s", encodings);
    Statistics<?> stats = meta.getStatistics();
    if (stats != null) {
      out.format(" ST:[%s]", stats);
    } else {
      out.format(" ST:[none]");
    }
    out.println();
  }

  static void showDetails(PrettyPrintWriter out, MessageType type, boolean showOriginalTypes) {
    List<String> cpath = new ArrayList<String>();
    for (Type ftype : type.getFields()) {
      showDetails(out, ftype, 0, type, cpath, showOriginalTypes);
    }
  }

  private static void showDetails(PrettyPrintWriter out, GroupType type, int depth, MessageType container,
      List<String> cpath, boolean showOriginalTypes) {
    String name = Strings.repeat(".", depth) + type.getName();
    Repetition rep = type.getRepetition();
    int fcount = type.getFieldCount();
    out.format("%s: %s F:%d%n", name, rep, fcount);

    cpath.add(type.getName());
    for (Type ftype : type.getFields()) {
      showDetails(out, ftype, depth + 1, container, cpath, showOriginalTypes);
    }
    cpath.remove(cpath.size() - 1);
  }

  private static void showDetails(PrettyPrintWriter out, PrimitiveType type, int depth, MessageType container,
      List<String> cpath, boolean showOriginalTypes) {
    String name = Strings.repeat(".", depth) + type.getName();
    Repetition rep = type.getRepetition();
    PrimitiveTypeName ptype = type.getPrimitiveTypeName();

    out.format("%s: %s %s", name, rep, ptype);
    if (showOriginalTypes) {
      OriginalType otype;
      try {
        otype = type.getOriginalType();
      } catch (Exception e) {
        otype = null;
      }
      if (otype != null)
        out.format(" O:%s", otype);
    } else {
      LogicalTypeAnnotation ltype = type.getLogicalTypeAnnotation();
      if (ltype != null)
        out.format(" L:%s", ltype);
    }

    if (container != null) {
      cpath.add(type.getName());
      String[] paths = cpath.toArray(new String[0]);
      cpath.remove(cpath.size() - 1);

      ColumnDescriptor desc = container.getColumnDescription(paths);

      int defl = desc.getMaxDefinitionLevel();
      int repl = desc.getMaxRepetitionLevel();
      out.format(" R:%d D:%d", repl, defl);
    }
    out.println();
  }

  private static void showDetails(PrettyPrintWriter out, Type type, int depth, MessageType container,
      List<String> cpath, boolean showOriginalTypes) {
    if (type instanceof GroupType) {
      showDetails(out, type.asGroupType(), depth, container, cpath, showOriginalTypes);
      return;
    } else if (type instanceof PrimitiveType) {
      showDetails(out, type.asPrimitiveType(), depth, container, cpath, showOriginalTypes);
      return;
    }
  }
}
