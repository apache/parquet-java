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
package redelm.redfile;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import redelm.data.simple.example.Paper;
import redelm.schema.MessageType;

import org.junit.Test;

import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.SchemaElement;

public class TestRedFileMetadataConverter {

  @Test
  public void testPageHeader() throws IOException {
    RedFileMetadataConverter redFileMetadataConverter = new RedFileMetadataConverter();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PageType type = PageType.DATA_PAGE;
    int compSize = 10;
    int uncSize = 20;
    PageHeader pageHeader = new PageHeader(type, uncSize, compSize);
    redFileMetadataConverter.writePageHeader(pageHeader, out);
    PageHeader readPageHeader = redFileMetadataConverter.readPageHeader(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(pageHeader, readPageHeader);
  }

  @Test
  public void testSchemaConverter() {
    RedFileMetadataConverter redFileMetadataConverter = new RedFileMetadataConverter();
    List<SchemaElement> redFileSchema = redFileMetadataConverter.toRedFileSchema(Paper.schema);
    MessageType schema = redFileMetadataConverter.fromRedFileSchema(redFileSchema);
    assertEquals(Paper.schema, schema);
  }

}
