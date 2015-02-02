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
package parquet.filter2.recordlevel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.filter2.compat.FilterCompat.Filter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class PhoneBookWriter {
  private static final String schemaString =
      "message user {\n"
          + "  required int64 id;\n"
          + "  optional binary name (UTF8);\n"
          + "  optional group location {\n"
          + "    optional double lon;\n"
          + "    optional double lat;\n"
          + "  }\n"
          + "  optional group phoneNumbers {\n"
          + "    repeated group phone {\n"
          + "      required int64 number;\n"
          + "      optional binary kind (UTF8);\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  private static final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

  public static class Location {
    private final Double lon;
    private final Double lat;

    public Location(Double lon, Double lat) {
      this.lon = lon;
      this.lat = lat;
    }

    public Double getLon() {
      return lon;
    }

    public Double getLat() {
      return lat;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Location location = (Location) o;

      if (lat != null ? !lat.equals(location.lat) : location.lat != null) return false;
      if (lon != null ? !lon.equals(location.lon) : location.lon != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = lon != null ? lon.hashCode() : 0;
      result = 31 * result + (lat != null ? lat.hashCode() : 0);
      return result;
    }
  }

  public static class PhoneNumber {
    private final long number;
    private final String kind;

    public PhoneNumber(long number, String kind) {
      this.number = number;
      this.kind = kind;
    }

    public long getNumber() {
      return number;
    }

    public String getKind() {
      return kind;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PhoneNumber that = (PhoneNumber) o;

      if (number != that.number) return false;
      if (kind != null ? !kind.equals(that.kind) : that.kind != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (number ^ (number >>> 32));
      result = 31 * result + (kind != null ? kind.hashCode() : 0);
      return result;
    }
  }

  public static class User {
    private final long id;
    private final String name;
    private final List<PhoneNumber> phoneNumbers;
    private final Location location;

    public User(long id, String name, List<PhoneNumber> phoneNumbers, Location location) {
      this.id = id;
      this.name = name;
      this.phoneNumbers = phoneNumbers;
      this.location = location;
    }

    public long getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public List<PhoneNumber> getPhoneNumbers() {
      return phoneNumbers;
    }

    public Location getLocation() {
      return location;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      User user = (User) o;

      if (id != user.id) return false;
      if (location != null ? !location.equals(user.location) : user.location != null) return false;
      if (name != null ? !name.equals(user.name) : user.name != null) return false;
      if (phoneNumbers != null ? !phoneNumbers.equals(user.phoneNumbers) : user.phoneNumbers != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (id ^ (id >>> 32));
      result = 31 * result + (name != null ? name.hashCode() : 0);
      result = 31 * result + (phoneNumbers != null ? phoneNumbers.hashCode() : 0);
      result = 31 * result + (location != null ? location.hashCode() : 0);
      return result;
    }
  }

  public static SimpleGroup groupFromUser(User user) {
    SimpleGroup root = new SimpleGroup(schema);
    root.append("id", user.getId());

    if (user.getName() != null) {
      root.append("name", user.getName());
    }

    if (user.getPhoneNumbers() != null) {
      Group phoneNumbers = root.addGroup("phoneNumbers");
      for (PhoneNumber number : user.getPhoneNumbers()) {
        Group phone = phoneNumbers.addGroup("phone");
        phone.append("number", number.getNumber());
        if (number.getKind() != null) {
          phone.append("kind", number.getKind());
        }
      }
    }

    if (user.getLocation() != null) {
      Group location = root.addGroup("location");
      if (user.getLocation().getLon() != null) {
        location.append("lon", user.getLocation().getLon());
      }
      if (user.getLocation().getLat() != null) {
        location.append("lat", user.getLocation().getLat());
      }
    }
    return root;
  }

  public static File writeToFile(List<User> users) throws IOException {
    File f = File.createTempFile("phonebook", ".parquet");
    f.deleteOnExit();
    if (!f.delete()) {
      throw new IOException("couldn't delete tmp file" + f);
    }

    writeToFile(f, users);

    return f;
  }

  public static void writeToFile(File f, List<User> users) throws IOException {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(new Path(f.getAbsolutePath()), conf, new GroupWriteSupport());
    for (User u : users) {
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
    writeToFile(f, TestRecordLevelFilters.makeUsers());
  }

}
