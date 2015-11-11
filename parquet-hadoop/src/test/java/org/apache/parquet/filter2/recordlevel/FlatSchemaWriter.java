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
package org.apache.parquet.filter2.recordlevel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class FlatSchemaWriter {
    private static final String schemaString =
            "message flatschema {\n"
                    + "  required int32 col0;\n"
                    + "  required int32 col1;\n"
                    + "  optional binary col2 (UTF8);\n"
                    + "  optional binary col3 (UTF8);\n"
                    + "  optional int32 col4;\n"
                    + "  optional double col5;\n"
                    + "  optional int32 col6;\n"
                    + "}\n";

    private static final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

    public static class FlatSchema {
        private final int col0;
        private final int col1;
        private final String col2;
        private final String col3;
        private final int col4;
        private final double col5;
        private final int col6;

        public FlatSchema(int pcol0, int pcol1, String pcol2, String pcol3, int pcol4, double pcol5, int pcol6) {
            this.col0 = pcol0;
            this.col1 = pcol1;
            this.col2 = pcol2;
            this.col3 = pcol3;
            this.col4 = pcol4;
            this.col5 = pcol5;
            this.col6 = pcol6;
        }

        public int col0() {
            return col0;
        }

        public int col1() {
            return col1;
        }

        public int col4() {
            return col4;
        }

        public int col6() {
            return col6;
        }

        public double col5() {
            return col5;
        }

        public String getCol2() {
            return col2;
        }

        public String getCol3() {
            return col3;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FlatSchema user = (FlatSchema) o;

            if (col0 != user.col0) return false;
            if (col1 != user.col1) return false;
            if (col4 != user.col4) return false;
            if (col5 != user.col5) return false;
            if (col6 != user.col6) return false;

            if (col2 != null ? !col2.equals(user.col2) : user.col2 != null) return false;
            if (col3 != null ? !col3.equals(user.col3) : user.col3 != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (col0 ^ (col0 >>> 32));
            result = 31 * result + (col2 != null ? col2.hashCode() : 0);
            result = 31 * result + (col3 != null ? col3.hashCode() : 0);
            result = 31 * result + (int)(col6 ^ (col6 >>> 32));
            return result;
        }
    }

    public static SimpleGroup groupFromUser(FlatSchema user) {
        SimpleGroup root = new SimpleGroup(schema);
        root.append("col0", user.col0());
        root.append("col1", user.col1());

        if (user.getCol2() != null) {
            root.append("col2", user.getCol2());
        }

        if (user.getCol3() != null) {
            root.append("col3", user.getCol3());
        }

        root.append("col4", user.col4());
        root.append("col5", user.col5());
        root.append("col6", user.col6());
        return root;
    }

    public static File writeToFile(List<FlatSchema> records) throws IOException {
        File f = File.createTempFile("phonebook2", ".parquet");
        f.deleteOnExit();
        if (!f.delete()) {
            throw new IOException("couldn't delete tmp file" + f);
        }

        writeToFile(f, records);

        return f;
    }

    public static void writeToFile(File f, List<FlatSchema> records) throws IOException {
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        ParquetWriter<Group> writer = new ParquetWriter<Group>(new Path(f.getAbsolutePath()), conf, new GroupWriteSupport());
        for (FlatSchema u : records) {
            writer.write(groupFromUser(u));
        }
        writer.close();
    }

    public static List<Group> readFile(File f, Filter filter) throws IOException {
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), new Path(f.getAbsolutePath()))
                        .withConf(conf)
                        .withFilter(filter)
                        .build();

        Group current;
        List<Group> users = new ArrayList<Group>();

        current = reader.read();
        while (current != null) {
            users.add(current);
            current = reader.read();
        }

        return users;
    }

    public static void main(String[] args) throws IOException {
        File f = new File(args[0]);
        writeToFile(f, TestRecordLevelFilters.makeRecords());
    }

}
