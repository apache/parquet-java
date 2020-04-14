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

package org.apache.parquet.cli.csv;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static java.lang.Math.min;

public class AvroCSV {

  private static final Pattern LONG = Pattern.compile("\\d+");
  private static final Pattern DOUBLE = Pattern.compile("\\d*\\.\\d*[dD]?");
  private static final Pattern FLOAT = Pattern.compile("\\d*\\.\\d*[fF]?");
  private static final int DEFAULT_INFER_LINES = 25;
  private static final Set<String> NO_REQUIRED_FIELDS = ImmutableSet.of();
  //As per the Avro specs mentioned here -http://avro.apache.org/docs/1.7.5/spec.html
  // It should start with [A-Za-z_] and subsequently contain only [A-Za-z0-9_]
  private static final Pattern AVRO_COMPATIBLE = Pattern.
      compile("^[A-Za-z_][A-Za-z\\d_]*$");

  static CSVReader newReader(InputStream incoming, CSVProperties props) {
    return new CSVReader(
        new InputStreamReader(incoming, Charset.forName(props.charset)),
        props.delimiter.charAt(0), props.quote.charAt(0),
        props.escape.charAt(0), props.linesToSkip,
        false /* strict quotes off: don't ignore unquoted strings */,
        true /* ignore leading white-space */ );
  }

  static CSVParser newParser(CSVProperties props) {
    return new CSVParser(
        props.delimiter.charAt(0), props.quote.charAt(0),
        props.escape.charAt(0),
        false /* strict quotes off: don't ignore unquoted strings */,
        true /* ignore leading white-space */ );
  }

  public static Schema inferNullableSchema(String name, InputStream incoming,
                                           CSVProperties props)
      throws IOException {
    return inferSchemaInternal(name, incoming, props, NO_REQUIRED_FIELDS, true);
  }

  public static Schema inferNullableSchema(String name, InputStream incoming,
                                           CSVProperties props,
                                           Set<String> requiredFields)
      throws IOException {
    return inferSchemaInternal(name, incoming, props, requiredFields, true);
  }

  public static Schema inferSchema(String name, InputStream incoming,
                                   CSVProperties props)
      throws IOException {
    return inferSchemaInternal(name, incoming, props, NO_REQUIRED_FIELDS, false);
  }

  public static Schema inferSchema(String name, InputStream incoming,
                                   CSVProperties props,
                                   Set<String> requiredFields)
      throws IOException {
    return inferSchemaInternal(name, incoming, props, requiredFields, false);
  }

  private static Schema inferSchemaInternal(String name, InputStream incoming,
                                            CSVProperties props,
                                            Set<String> requiredFields,
                                            boolean makeNullable)
      throws IOException {
    CSVReader reader = newReader(incoming, props);

    String[] header;
    String[] line;
    if (props.useHeader) {
      // read the header and then the first line
      header = reader.readNext();
      line = reader.readNext();
      Objects.requireNonNull(line, "No content to infer schema");

    } else if (props.header != null) {
      header = newParser(props).parseLine(props.header);
      line = reader.readNext();
      Objects.requireNonNull(line, "No content to infer schema");

    } else {
      // use the first line to create a header
      line = reader.readNext();
      Objects.requireNonNull(line, "No content to infer schema");
      header = new String[line.length];
      for (int i = 0; i < line.length; i += 1) {
        header[i] = "field_" + String.valueOf(i);
      }
    }

    Schema.Type[] types = new Schema.Type[header.length];
    String[] values = new String[header.length];
    boolean[] nullable = new boolean[header.length];
    boolean[] empty = new boolean[header.length];

    for (int processed = 0; processed < DEFAULT_INFER_LINES; processed += 1) {
      if (line == null) {
        break;
      }

      for (int i = 0; i < header.length; i += 1) {
        if (i < line.length) {
          if (types[i] == null) {
            types[i] = inferFieldType(line[i]);
            if (types[i] != null) {
              // keep track of the value used
              values[i] = line[i];
            }
          }

          if (line[i] == null) {
            nullable[i] = true;
          } else if (line[i].isEmpty()) {
            empty[i] = true;
          }
        } else {
          // no value results in null
          nullable[i] = true;
        }
      }

      line = reader.readNext();
    }

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(name).fields();

    // types may be missing, but fieldSchema will return a nullable string
    for (int i = 0; i < header.length; i += 1) {
      if (header[i] == null) {
        throw new RuntimeException("Bad header for field " + i + ": null");
      }

      String fieldName = header[i].trim();

      if (fieldName.isEmpty()) {
        throw new RuntimeException(
            "Bad header for field " + i + ": \"" + fieldName + "\"");
      } else if(!isAvroCompatibleName(fieldName)) {
    	  throw new RuntimeException(
              "Bad header for field, should start with a character " +
              "or _ and can contain only alphanumerics and _ " +
              i + ": \"" + fieldName + "\"");
      }

      // the empty string is not considered null for string fields
      boolean foundNull = (nullable[i] ||
          (empty[i] && types[i] != Schema.Type.STRING));

      if (requiredFields.contains(fieldName)) {
        if (foundNull) {
          throw new RuntimeException("Found null value for required field: " +
              fieldName + " (" + types[i] + ")");
        }
        fieldAssembler = fieldAssembler.name(fieldName)
            .doc("Type inferred from '" + sample(values[i]) + "'")
            .type(schema(types[i], false)).noDefault();
      } else {
        SchemaBuilder.GenericDefault<Schema> defaultBuilder = fieldAssembler.name(fieldName)
            .doc("Type inferred from '" + sample(values[i]) + "'")
            .type(schema(types[i], makeNullable || foundNull));
        if (makeNullable || foundNull) {
          fieldAssembler = defaultBuilder.withDefault(null);
        } else {
          fieldAssembler = defaultBuilder.noDefault();
        }
      }
    }
    return fieldAssembler.endRecord();
  }

  private static final CharMatcher NON_PRINTABLE = CharMatcher
      .inRange('\u0020', '\u007e').negate();

  private static String sample(String value) {
    if (value != null) {
      return NON_PRINTABLE.replaceFrom(
          value.subSequence(0, min(50, value.length())), '.');
    } else {
      return "null";
    }
  }

  /**
   * Create a {@link Schema} for the given type. If the type is null,
   * the schema will be a nullable String. If isNullable is true, the returned
   * schema will be nullable.
   *
   * @param type a {@link Schema.Type} compatible with {@code Schema.create}
   * @param makeNullable If {@code true}, the return type will be nullable
   * @return a {@code Schema} for the given {@code Schema.Type}
   * @see Schema#create(org.apache.avro.Schema.Type)
   */
  private static Schema schema(Schema.Type type, boolean makeNullable) {
    Schema schema = Schema.create(type == null ? Schema.Type.STRING : type);
    if (makeNullable || type == null) {
      schema = Schema.createUnion(Lists.newArrayList(
          Schema.create(Schema.Type.NULL), schema));
    }
    return schema;
  }

  private static Schema.Type inferFieldType(String example) {
    if (example == null || example.isEmpty()) {
      return null; // not enough information
    } else if (LONG.matcher(example).matches()) {
      return Schema.Type.LONG;
    } else if (DOUBLE.matcher(example).matches()) {
      return Schema.Type.DOUBLE;
    } else if (FLOAT.matcher(example).matches()) {
      return Schema.Type.FLOAT;
    }
    return Schema.Type.STRING;
  }

  /**
   * Returns true if the name does not contain characters that are known to be
   * incompatible with the specs defined in Avro schema.
   *
   * @param name a String field name to check
   * @return will return true if the name is Avro compatible ,false if not
   */
  private static boolean isAvroCompatibleName(String name) {
    return AVRO_COMPATIBLE.matcher(name).matches();
  }
}
