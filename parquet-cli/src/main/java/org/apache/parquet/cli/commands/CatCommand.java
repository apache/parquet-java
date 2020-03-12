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
package org.apache.parquet.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Expressions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.StringKeyIdRetriever;
import org.slf4j.Logger;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.apache.parquet.cli.util.Expressions.select;

@Parameters(commandDescription = "Print the first N records from a file")
public class CatCommand extends BaseCommand {

  @Parameter(description = "<file>")
  List<String> sourceFiles;

  @Parameter(names={"-n", "--num-records"},
      description="The number of records to print")
  long numRecords;

  @Parameter(
      names = {"-c", "--column", "--columns"},
      description = "List of columns")
  List<String> columns;
  
  @Parameter(names={"-e", "--encrypted-file"},
      description="Cat an encrypted Parquet file")
  boolean encrypt = false;

  @Parameter(names={"--key"},
      description="Encryption key (base64 string)")
  String encodedKey;

  public CatCommand(Logger console, long defaultNumRecords) {
    super(console);
    this.numRecords = defaultNumRecords;
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(
        sourceFiles != null && !sourceFiles.isEmpty(),
        "Missing file name");
    Preconditions.checkArgument(sourceFiles.size() == 1,
        "Only one file can be given");

    final String source = sourceFiles.get(0);
    
    FileDecryptionProperties dSetup = null;
    if (encrypt) {
      byte[] keyBytes;
      if (null == encodedKey) {
        keyBytes = new byte[16];
        for (byte i=0; i < 16; i++) {keyBytes[i] = i;}
        String sampleKey = Base64.getEncoder().encodeToString(keyBytes);
        console.info("Decrypting with a sample key: " +sampleKey);
      }
      else {
        keyBytes = Base64.getDecoder().decode(encodedKey);
      }

      StringKeyIdRetriever kr = new StringKeyIdRetriever();
      kr.putKey("kf", keyBytes);

      for (int c=0; c < 20; c++) {

        byte[] colKeyBytes = new byte[16]; 
        for (byte i=0; i < 16; i++) {colKeyBytes[i] = (byte)(i*(c+2));}

        kr.putKey("kc"+c, colKeyBytes);
      }


      byte[] aad = source.getBytes(StandardCharsets.UTF_8);
      console.info("AAD Prefix: "+source+". Len: "+aad.length);


      dSetup = FileDecryptionProperties.builder()
          .withKeyRetriever(kr)
          //.withPlaintextFilesAllowed()
          //.withAADPrefix(aad)
          .build();
    }

    Configuration conf = getConf();

    Schema schema = getAvroSchema(source, dSetup);
    
    Schema projection = Expressions.filterSchema(schema, columns);

    Iterable<Object> reader = openDataFile(source, projection, dSetup);
    boolean threw = true;
    long count = 0;
    try {
      for (Object record : reader) {
        if (numRecords > 0 && count >= numRecords) {
          break;
        }
        if (columns == null || columns.size() != 1) {
          console.info(String.valueOf(record));
        } else {
          console.info(String.valueOf(select(projection, record, columns.get(0))));
        }
        count += 1;
      }
      threw = false;
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed on record " + count, e);
    } finally {
      if (reader instanceof Closeable) {
        Closeables.close((Closeable) reader, threw);
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Show the first 10 records in file \"data.avro\":",
        "data.avro",
        "# Show the first 50 records in file \"data.parquet\":",
        "data.parquet -n 50"
    );
  }
}

