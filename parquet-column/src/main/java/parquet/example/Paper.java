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
package parquet.example;

import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REPEATED;
import static parquet.schema.Type.Repetition.REQUIRED;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

/**
 * Examples from the Dremel Paper
 *
 * @author Julien Le Dem
 *
 */
public class Paper {
  public static final MessageType schema =
      new MessageType("Document",
          new PrimitiveType(REQUIRED, INT64, "DocId"),
          new GroupType(OPTIONAL, "Links",
              new PrimitiveType(REPEATED, INT64, "Backward"),
              new PrimitiveType(REPEATED, INT64, "Forward")
              ),
          new GroupType(REPEATED, "Name",
              new GroupType(REPEATED, "Language",
                  new PrimitiveType(REQUIRED, BINARY, "Code"),
                  new PrimitiveType(OPTIONAL, BINARY, "Country")),
              new PrimitiveType(OPTIONAL, BINARY, "Url")));

  public static final MessageType schema2 =
      new MessageType("Document",
          new PrimitiveType(REQUIRED, INT64, "DocId"),
          new GroupType(REPEATED, "Name",
              new GroupType(REPEATED, "Language",
                  new PrimitiveType(OPTIONAL, BINARY, "Country"))));

  public static final MessageType schema3 =
      new MessageType("Document",
          new PrimitiveType(REQUIRED, INT64, "DocId"),
          new GroupType(OPTIONAL, "Links",
              new PrimitiveType(REPEATED, INT64, "Backward"),
              new PrimitiveType(REPEATED, INT64, "Forward")
              ));

  public static final SimpleGroup r1 = new SimpleGroup(schema);
  public static final SimpleGroup r2 = new SimpleGroup(schema);
  ////r1
  //DocId: 10
  //Links
  //  Forward: 20
  //  Forward: 40
  //  Forward: 60
  //Name
  //  Language
  //    Code: 'en-us'
  //    Country: 'us'
  //  Language
  //    Code: 'en'
  //  Url: 'http://A'
  //Name
  //  Url: 'http://B'
  //Name
  //  Language
  //    Code: 'en-gb'
  //    Country: 'gb'
  static {
    r1.add("DocId", 10l);
    r1.addGroup("Links")
      .append("Forward", 20l)
      .append("Forward", 40l)
      .append("Forward", 60l);
    Group name = r1.addGroup("Name");
    {
      name.addGroup("Language")
        .append("Code", "en-us")
        .append("Country", "us");
      name.addGroup("Language")
        .append("Code", "en");
      name.append("Url", "http://A");
    }
    name = r1.addGroup("Name");
    {
      name.append("Url", "http://B");
    }
    name = r1.addGroup("Name");
    {
      name.addGroup("Language")
        .append("Code", "en-gb")
        .append("Country", "gb");
    }
  }
  ////r2
  //DocId: 20
  //Links
  // Backward: 10
  // Backward: 30
  // Forward:  80
  //Name
  // Url: 'http://C'
  static {
    r2.add("DocId", 20l);
    r2.addGroup("Links")
      .append("Backward", 10l)
      .append("Backward", 30l)
      .append("Forward", 80l);
    r2.addGroup("Name")
      .append("Url", "http://C");
  }

  public static final SimpleGroup pr1 = new SimpleGroup(schema2);
  public static final SimpleGroup pr2 = new SimpleGroup(schema2);
  ////r1
  //DocId: 10
  //Name
  //  Language
  //    Country: 'us'
  //  Language
  //Name
  //Name
  //  Language
  //    Country: 'gb'
  static {
    pr1.add("DocId", 10l);
    Group name = pr1.addGroup("Name");
    {
      name.addGroup("Language")
        .append("Country", "us");
      name.addGroup("Language");
    }
    name = pr1.addGroup("Name");
    name = pr1.addGroup("Name");
    {
      name.addGroup("Language")
        .append("Country", "gb");
    }
  }

  ////r2
  //DocId: 20
  //Name
  static {
    pr2.add("DocId", 20l);
    pr2.addGroup("Name");
  }
}
