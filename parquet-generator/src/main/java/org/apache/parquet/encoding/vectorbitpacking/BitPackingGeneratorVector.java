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
package org.apache.parquet.encoding.vectorbitpacking;

import java.io.*;

/**
 * This class generates vector bit packers that pack the most significant bit first.
 * The result of the generation is checked in. To regenerate the code run this class and check in the result.
 */
public class BitPackingGeneratorVector {
  private static final String CLASS_NAME_PREFIX_FOR_INT = "ByteBitPackingVector";
  private static final String CLASS_NAME_PREFIX_FOR_LONG = "ByteBitPackingVectorForLong";

  public static void main(String[] args) throws Exception {
    String basePath = args[0];
    //TODO: Int for Big Endian
    //generateScheme(false, true, basePath);

    // Int for Little Endian
    generateScheme(false, false, basePath);

    //TODO: Long for Big Endian
    //generateScheme(true, true, basePath);

    //TODO: Long for Little Endian
    //generateScheme(true, false, basePath);
  }

  private static void generateScheme(boolean isLong, boolean msbFirst,
                                     String basePath) throws IOException {
    String baseClassName = isLong ? CLASS_NAME_PREFIX_FOR_LONG : CLASS_NAME_PREFIX_FOR_INT;
    String className = msbFirst ? (baseClassName + "BE") : (baseClassName + "LE");

    final File file = new File(basePath + "/org/apache/parquet/column/values/bitpacking/" + className + ".java").getAbsoluteFile();
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    ClassLoader classLoader = BitPackingGeneratorVector.class.getClassLoader();
    try (InputStream inputStream = classLoader.getResourceAsStream("ByteBitPackingVectorLE");
         OutputStream output = new FileOutputStream(file, false)) {
      byte[] bytes = new byte[inputStream.available()];
      inputStream.read(bytes);
      output.write(bytes);
    }
  }
}
