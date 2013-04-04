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
package parquet.format.converter;

import static org.junit.Assert.assertEquals;
import static parquet.format.Util.readPageHeader;
import static parquet.format.Util.writePageHeader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import parquet.column.Encoding;
import parquet.example.Paper;
import parquet.format.FieldRepetitionType;
import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.SchemaElement;
import parquet.format.Type;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

public class TestParquetMetadataConverter {

  @Test
  public void testPageHeader() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PageType type = PageType.DATA_PAGE;
    int compSize = 10;
    int uncSize = 20;
    PageHeader pageHeader = new PageHeader(type, uncSize, compSize);
    writePageHeader(pageHeader, out);
    PageHeader readPageHeader = readPageHeader(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(pageHeader, readPageHeader);
  }

  @Test
  public void testSchemaConverter() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(Paper.schema);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema);
    assertEquals(Paper.schema, schema);
  }

  @Test
  public void testEnumEquivalence() {
    ParquetMetadataConverter c = new ParquetMetadataConverter();
    for (Encoding encoding : Encoding.values()) {
      assertEquals(encoding, c.getEncoding(c.getEncoding(encoding)));
    }
    for (parquet.format.Encoding encoding : parquet.format.Encoding.values()) {
      assertEquals(encoding, c.getEncoding(c.getEncoding(encoding)));
    }
    for (Repetition repetition : Repetition.values()) {
      assertEquals(repetition, c.fromParquetRepetition(c.toParquetRepetition(repetition)));
    }
    for (FieldRepetitionType repetition : FieldRepetitionType.values()) {
      assertEquals(repetition, c.toParquetRepetition(c.fromParquetRepetition(repetition)));
    }
    for (PrimitiveTypeName primitiveTypeName : PrimitiveTypeName.values()) {
      assertEquals(primitiveTypeName, c.getPrimitive(c.getType(primitiveTypeName)));
    }
    for (Type type : Type.values()) {
      assertEquals(type, c.getType(c.getPrimitive(type)));
    }
  }

}
