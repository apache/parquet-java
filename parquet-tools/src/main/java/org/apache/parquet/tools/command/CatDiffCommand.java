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

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.Main;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.bitbucket.cowwoc.diffmatchpatch.DiffMatchPatch;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;


public class CatDiffCommand extends ArgsOnlyCommand {

    public static final String[] USAGE = new String[] {
            "<input1> <input2>",
            "where <input1> and <input2> are the parquet files to compare print to stdout"
    };

    public CatDiffCommand(){
        super(2,2);
    }

    // Reset
    private static final String RESET = "\033[0m";  // Text Reset
    private static final String RED = "\033[0;31m"; // RED
    private static final String GREEN = "\033[0;32m";   // GREEN

    private static final int DIFF_LENGTH_THRESHOLD = 200;
    private static final String DIFF_OVER_THRESHOLD_SEPARATOR = "\n.........................\n";


    @Override
    public String[] getUsageDescription() {
        return USAGE;
    }

    @Override
    public String getCommandDescription() {
        return "Prints the difference between two Parquet files.";
    }

    @Override
    public void execute(CommandLine options) throws Exception {
        super.execute(options);

        String[] args = options.getArgs();
        String input1 = args[0];
        String input2 = args[1];

        String input1Json = getParquetInJson(input1);
        String input2Json = getParquetInJson(input2);

        DiffMatchPatch dmp = new DiffMatchPatch();
        List<DiffMatchPatch.Diff> diffs = dmp.diffMain(input1Json, input2Json);

        PrintWriter writer = new PrintWriter(Main.out, true);
        writer.println("There are " + diffs.size() + " diffs");

        for(DiffMatchPatch.Diff currDiff : diffs) {
            String diffText = currDiff.text.length() > DIFF_LENGTH_THRESHOLD ?
                    currDiff.text.substring(0, DIFF_LENGTH_THRESHOLD) + DIFF_OVER_THRESHOLD_SEPARATOR: currDiff.text;
            if(currDiff.operation == DiffMatchPatch.Operation.DELETE) {
                writer.write("-" + RED + diffText + RESET);
            } else if(currDiff.operation == DiffMatchPatch.Operation.INSERT) {
                writer.write("+" + GREEN + diffText + RESET);
            } else {
                writer.write(RESET + diffText + RESET);
            }
            writer.println();
        }
        writer.println();

    }

    private String getParquetInJson(String input) throws IOException {
        ParquetReader<SimpleRecord> reader = null;
        //StringBuilder sb = new StringBuilder();
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        try {
            reader = ParquetReader.builder(new SimpleReadSupport(), new Path(input)).build();
            ParquetMetadata metadata = ParquetFileReader.readFooter(new Configuration(), new Path(input));
            JsonRecordFormatter.JsonGroupFormatter formatter = JsonRecordFormatter.fromSchema(metadata.getFileMetaData().getSchema());

            for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
                //sb.append(formatter.formatRecord(value));
                value.prettyPrint(writer);
            }
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ex) {
                }
            }
        }
        return sw.toString();

    }

}
