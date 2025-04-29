/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.avro;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class ReflectClassValidator {

  abstract void validate(String className);

  static class PackageValidator extends ReflectClassValidator {

    private final List<String> trustedPackagePrefixes = Stream.of(AvroConverters.SERIALIZABLE_PACKAGES)
        .map(p -> p.endsWith(".") ? p : p + ".")
        .collect(Collectors.toList());
    private final boolean trustAllPackages =
        trustedPackagePrefixes.size() == 1 && "*".equals(trustedPackagePrefixes.get(0));
    // The primitive "class names" based on Class.isPrimitive()
    private static final Set<String> PRIMITIVES = new HashSet<>(Arrays.asList(
        Boolean.TYPE.getName(),
        Character.TYPE.getName(),
        Byte.TYPE.getName(),
        Short.TYPE.getName(),
        Integer.TYPE.getName(),
        Long.TYPE.getName(),
        Float.TYPE.getName(),
        Double.TYPE.getName(),
        Void.TYPE.getName()));

    @Override
    public void validate(String className) {
      if (trustAllPackages || PRIMITIVES.contains(className)) {
        return;
      }

      for (String packagePrefix : trustedPackagePrefixes) {
        if (className.startsWith(packagePrefix)) {
          return;
        }
      }

      forbiddenClass(className);
    }
  }

  static class ClassValidator extends ReflectClassValidator {
    private final Set<String> trustedClassNames = new HashSet<>();

    ClassValidator(String... classNames) {
      for (String className : classNames) {
        addTrustedClassName(className);
      }
    }

    void addTrustedClassName(String className) {
      trustedClassNames.add(className);
    }

    @Override
    void validate(String className) {
      if (!trustedClassNames.contains(className)) {
        forbiddenClass(className);
      }
    }
  }

  private static void forbiddenClass(String className) {
    throw new SecurityException("Forbidden " + className
        + "! This class is not trusted to be included in Avro schema using java-class or java-key-class."
        + " Please set the Parquet/Hadoop configuration parquet.avro.serializable.classes"
        + " with the classes you trust.");
  }
}
