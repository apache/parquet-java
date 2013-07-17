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
package parquet.pig;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.Log;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.pig.convert.TupleRecordMaterializer;
import parquet.schema.MessageType;

/**
 * Read support for Pig Tuple
 * a Pig MetaDataBlock is expected in the initialization call
 *
 * @author Julien Le Dem
 *
 */
public class TupleReadSupport extends ReadSupport<Tuple> {
  static final String PARQUET_PIG_REQUESTED_SCHEMA = "parquet.pig.requested.schema";
  static final String PARQUET_PIG_NUMBERS_DEFAULT_TO_ZERO = "parquet.pig.numbers.default.to.zero";

  private static final Log LOG = Log.getLog(TupleReadSupport.class);

  private static final PigSchemaConverter schemaConverter = new PigSchemaConverter();


  /**
   * @param configuration the configuration for the current job
   * @return the pig schema requested by the user or null if none.
   */
  static Schema getRequestedPigSchema(Configuration configuration) {
    Schema pigSchema = null;
    try {
      String schemaStr = configuration.get(PARQUET_PIG_REQUESTED_SCHEMA);
      pigSchema = (Schema)ObjectSerializer.deserialize(schemaStr);
    } catch (IOException ioe) {
      throw new SchemaConversionException("could not get pig schema from configuration ", ioe);
    }
    return pigSchema;
  }

  static Schema parsePigSchema(String pigSchemaString) {
    try {
      return pigSchemaString == null ? null : Utils.getSchemaFromString(pigSchemaString);
    } catch (ParserException e) {
      throw new SchemaConversionException("could not parse Pig schema: " + pigSchemaString, e);
    }
  }

  /**
   * @param fileSchema the parquet schema from the file
   * @param keyValueMetaData the extra meta data from the file
   * @return the pig schema according to the file
   */
  static Schema getPigSchemaFromFile(MessageType fileSchema, Map<String, String> keyValueMetaData) {
    PigMetaData pigMetaData = PigMetaData.fromMetaData(keyValueMetaData);
    // TODO: if no Pig schema in file: generate one from the Parquet schema
    return parsePigSchema(pigMetaData.getPigSchema());
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    Schema requestedPigSchema = getRequestedPigSchema(configuration);
    if (requestedPigSchema == null) {
      return new ReadContext(fileSchema);
    } else {
      // project the file schema according to the requested Pig schema
      MessageType parquetRequestedSchema =
          schemaConverter.filter(
          fileSchema,
          requestedPigSchema);
      return new ReadContext(parquetRequestedSchema);
    }
  }

  @Override
  public RecordMaterializer<Tuple> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    MessageType requestedSchema = readContext.getRequestedSchema();
    Schema requestedPigSchema = getRequestedPigSchema(configuration);
    if (requestedPigSchema == null) {
      requestedPigSchema = getPigSchemaFromFile(fileSchema, keyValueMetaData);
    }
    boolean numbersDefaultToZero = configuration.getBoolean(PARQUET_PIG_NUMBERS_DEFAULT_TO_ZERO, false);
    if (numbersDefaultToZero) {
      LOG.info("Numbers will default to 0 instead of NULL");
    }
    return new TupleRecordMaterializer(requestedSchema, requestedPigSchema, numbersDefaultToZero);
  }

}
