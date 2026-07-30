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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.util.RawUtils;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.Test;

public class ShowFooterCommandTest extends ParquetFileTest {
  @Test
  public void testShowDirectoryCommand() throws IOException {
    File file = parquetFile();
    ShowFooterCommand command = new ShowFooterCommand(createLogger());
    command.target = file.getAbsolutePath();
    command.raw = false;
    command.setConf(new Configuration());
    assertThat(command.run()).isZero();

    command.raw = true;
    assertThat(command.run()).isZero();
  }

  /**
   * The file a footer was read from is not part of the printed footer, even though this mapper serializes fields
   * rather than getters and cannot see the relocated annotations of parquet-hadoop.
   */
  @Test
  public void testInputFileNotPrinted() throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(new Path(parquetFile().getAbsolutePath()), new Configuration());
    ParquetMetadata footer;
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      footer = reader.getFooter();
    }
    assertThat(footer.getInputFile()).isNotNull();

    ObjectMapper mapper = RawUtils.createObjectMapper();
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    mapper.addMixIn(ParquetMetadata.class, ShowFooterCommand.MixIn.class);
    assertThat(mapper.writeValueAsString(footer)).doesNotContain("inputFile");
  }
}
