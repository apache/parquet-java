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
package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Reads nation.dict-malformed.parquet of apache/parquet-testing. An old parquet-mr wrote it without the size of the
 * dictionary page header in the length of the column chunks, so the chunks of name and comment_col are 15 bytes
 * shorter than their pages and the missing bytes have to be read from the file after the end of the chunk.
 */
public class TestOldWriterDictionaryChunks {

  private static final Path FILE = createPathFromCP("/nation.dict-malformed.parquet");

  private static final int[] REGIONS = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1};

  private static final String[] NAMES = {
    "ALGERIA",
    "ARGENTINA",
    "BRAZIL",
    "CANADA",
    "EGYPT",
    "ETHIOPIA",
    "FRANCE",
    "GERMANY",
    "INDIA",
    "INDONESIA",
    "IRAN",
    "IRAQ",
    "JAPAN",
    "JORDAN",
    "KENYA",
    "MOROCCO",
    "MOZAMBIQUE",
    "PERU",
    "CHINA",
    "ROMANIA",
    "SAUDI ARABIA",
    "VIETNAM",
    "RUSSIA",
    "UNITED KINGDOM",
    "UNITED STATES",
  };

  private static final String[] COMMENTS = {
    " haggle. carefully final deposits detect slyly agai",
    "al foxes promise slyly according to the regular accounts. bold requests alon",
    "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ",
    "eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold",
    "y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d",
    "ven packages wake quickly. regu",
    "refully final requests. regular, ironi",
    "l platelets. regular accounts x-ray: unusual, regular acco",
    "ss excuses cajole slyly across the packages. deposits print aroun",
    " slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull",
    "efully alongside of the slyly final dependencies. ",
    "nic deposits boost atop the quickly final requests? quickly regula",
    "ously. final, express gifts cajole a",
    "ic deposits are blithely about the carefully regular pa",
    " pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t",
    "rns. blithely bold courts among the closely regular packages use furiously bold platelets?",
    "s. ironic, unusual asymptotes wake blithely r",
    "platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun",
    "c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos",
    "ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account",
    "ts. silent requests haggle. closely express packages sleep across the blithely",
    "hely enticingly express accounts. even, final ",
    " requests against the platelets use never according to the quickly regular pint",
    "eans boost carefully special requests. accounts are. carefull",
    "y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be",
  };

  private static Path createPathFromCP(String path) {
    try {
      return new Path(
          TestOldWriterDictionaryChunks.class.getResource(path).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Group> read(boolean vectoredIo, String requestedSchema) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED, vectoredIo);
    if (requestedSchema != null) {
      conf.set(ReadSupport.PARQUET_READ_SCHEMA, requestedSchema);
    }
    List<Group> rows = new ArrayList<>();
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), FILE)
        .withConf(conf)
        .build()) {
      Group row;
      while ((row = reader.read()) != null) {
        rows.add(row);
      }
    }
    return rows;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAllColumns(boolean vectoredIo) throws IOException {
    List<Group> rows = read(vectoredIo, null);

    assertThat(rows).hasSize(25);
    for (int i = 0; i < rows.size(); i++) {
      Group row = rows.get(i);
      assertThat(row.getInteger("nation_key", 0)).isEqualTo(i);
      assertThat(row.getString("name", 0)).isEqualTo(NAMES[i]);
      assertThat(row.getInteger("region_key", 0)).isEqualTo(REGIONS[i]);
      assertThat(row.getString("comment_col", 0)).isEqualTo(COMMENTS[i]);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNameOnly(boolean vectoredIo) throws IOException {
    List<Group> rows = read(vectoredIo, "message m { optional binary name; }");

    assertThat(rows).hasSize(25);
    for (int i = 0; i < rows.size(); i++) {
      assertThat(rows.get(i).getString("name", 0)).isEqualTo(NAMES[i]);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCommentOnly(boolean vectoredIo) throws IOException {
    List<Group> rows = read(vectoredIo, "message m { optional binary comment_col; }");

    assertThat(rows).hasSize(25);
    for (int i = 0; i < rows.size(); i++) {
      assertThat(rows.get(i).getString("comment_col", 0)).isEqualTo(COMMENTS[i]);
    }
  }
}
