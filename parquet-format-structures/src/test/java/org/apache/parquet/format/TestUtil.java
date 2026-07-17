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
package org.apache.parquet.format;

import static java.util.Arrays.asList;
import static org.apache.parquet.format.Util.readFileMetaData;
import static org.apache.parquet.format.Util.writeFileMetaData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.parquet.format.Util.DefaultFileMetaDataConsumer;
import org.junit.Test;

public class TestUtil {

  @Test
  public void testReadFileMetadata() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FileMetaData md = new FileMetaData(
        1,
        asList(new SchemaElement("foo")),
        10,
        asList(
            new RowGroup(asList(new ColumnChunk(0), new ColumnChunk(1)), 10, 5),
            new RowGroup(asList(new ColumnChunk(2), new ColumnChunk(3)), 11, 5)));
    writeFileMetaData(md, baos);
    FileMetaData md2 = readFileMetaData(in(baos));
    FileMetaData md3 = new FileMetaData();
    readFileMetaData(in(baos), new DefaultFileMetaDataConsumer(md3));
    FileMetaData md4 = new FileMetaData();
    readFileMetaData(in(baos), new DefaultFileMetaDataConsumer(md4), true);
    FileMetaData md5 = readFileMetaData(in(baos), true);
    FileMetaData md6 = readFileMetaData(in(baos), false);
    assertThat(md2).isEqualTo(md3).isEqualTo(md);
    assertThat(md4.getRow_groups()).isNull();
    assertThat(md5.getRow_groups()).isNull();
    assertThat(md4).isEqualTo(md5);
    md4.setRow_groups(md.getRow_groups());
    md5.setRow_groups(md.getRow_groups());
    assertThat(md4).isEqualTo(md5).isEqualTo(md6).isEqualTo(md);
  }

  @Test
  public void testInvalidPageHeader() throws IOException {
    PageHeader ph = new PageHeader(PageType.DATA_PAGE, 100, -50);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Util.writePageHeader(ph, out);

    assertThatThrownBy(() -> Util.readPageHeader(in(out)))
        .isInstanceOf(InvalidParquetMetadataException.class)
        .hasMessageContaining("Compressed page size");
  }

  private ByteArrayInputStream in(ByteArrayOutputStream baos) {
    return new ByteArrayInputStream(baos.toByteArray());
  }
}
