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
package org.apache.parquet.proto;

import static org.apache.parquet.proto.ByteBuddyCodeGen.CodeGenUtils.Reflection;

import com.google.common.collect.MapMaker;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.MethodList;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.Removal;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackSize;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.collection.ArrayFactory;
import net.bytebuddy.implementation.bytecode.constant.DoubleConstant;
import net.bytebuddy.implementation.bytecode.constant.FloatConstant;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.constant.JavaConstantValue;
import net.bytebuddy.implementation.bytecode.constant.LongConstant;
import net.bytebuddy.implementation.bytecode.constant.TextConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.Handle;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaConstant;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.proto.ByteBuddyCodeGen.CodeGenUtils.Codegen;
import org.apache.parquet.proto.ByteBuddyCodeGen.CodeGenUtils.Implementations;
import org.apache.parquet.proto.ByteBuddyCodeGen.CodeGenUtils.LocalVar;
import org.apache.parquet.proto.ProtoMessageConverter.AddRepeatedFieldParentValueContainer;
import org.apache.parquet.proto.ProtoMessageConverter.ListConverter;
import org.apache.parquet.proto.ProtoMessageConverter.MapConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ParentValueContainer;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoBinaryConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoBoolValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoBooleanConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoBytesValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoDateConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoDoubleConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoDoubleValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoEnumConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoFloatConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoFloatValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoInt32ValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoInt64ValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoIntConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoLongConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoStringConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoStringValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoTimeConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoTimestampConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoUInt32ValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.ProtoUInt64ValueConverter;
import org.apache.parquet.proto.ProtoMessageConverter.SetFieldParentValueContainer;
import org.apache.parquet.schema.MessageType;

public class ByteBuddyCodeGen {
  private static final AtomicLong BYTE_BUDDY_CLASS_SEQUENCE = new AtomicLong();

  private static final GenerateMessageClasses GeneratedMessageV3 =
      GenerateMessageClasses.resolve("com.google.protobuf.GeneratedMessageV3");
  private static final GenerateMessageClasses GeneratedMessage =
      GenerateMessageClasses.resolve("com.google.protobuf.GeneratedMessage");

  static class CodeGenException extends RuntimeException {
    public CodeGenException() {
      super();
    }

    public CodeGenException(String message) {
      super(message);
    }

    public CodeGenException(String message, Throwable cause) {
      super(message, cause);
    }

    public CodeGenException(Throwable cause) {
      super(cause);
    }
  }

  static boolean isGeneratedMessage(Class<?> protoMessage) {
    return protoMessage != null
        && (GeneratedMessage.isGeneratedMessage(protoMessage)
            || GeneratedMessageV3.isGeneratedMessage(protoMessage));
  }

  static boolean isExtendableMessage(Class<?> protoMessage) {
    return protoMessage != null
        && (GeneratedMessage.isExtendableMessage(protoMessage)
            || GeneratedMessageV3.isExtendableMessage(protoMessage));
  }

  static class GenerateMessageClasses {
    private final Class<?> classGeneratedMessage;
    private final Class<?> classExtendableMessage;

    private GenerateMessageClasses(Class<?> classGeneratedMessage, Class<?> classExtendableMessage) {
      this.classGeneratedMessage = classGeneratedMessage;
      this.classExtendableMessage = classExtendableMessage;
    }

    static GenerateMessageClasses resolve(String generatedMessageClassName) {
      Optional<Class<?>> generatedMessage = ReflectionUtil.classForName(generatedMessageClassName);
      Optional<Class<?>> extendableMessage =
          ReflectionUtil.classForName(generatedMessageClassName + "$ExtendableMessage");
      if (generatedMessage.isPresent() && extendableMessage.isPresent()) {
        return new GenerateMessageClasses(generatedMessage.get(), extendableMessage.get());
      } else {
        return new GenerateMessageClasses(null, null);
      }
    }

    public boolean isGeneratedMessage(Class<?> clazz) {
      return classGeneratedMessage != null && clazz != null && classGeneratedMessage.isAssignableFrom(clazz);
    }

    public boolean isExtendableMessage(Class<?> clazz) {
      return classExtendableMessage != null && clazz != null && classExtendableMessage.isAssignableFrom(clazz);
    }
  }

  static boolean isByteBuddyAvailable(boolean failIfNot) {
    try {
      Class.forName("net.bytebuddy.ByteBuddy", false, ByteBuddyCodeGen.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException e) {
      if (failIfNot) {
        throw new UnsupportedOperationException("ByteBuddy is not available", e);
      }
      return false;
    }
  }

  static class CodeGenUtils {
    // resolve reflection methods early, so tests would fail fast should anything is changed in interfaces/classes
    static final ResolvedReflection Reflection = new ResolvedReflection();

    static class ResolvedReflection {
      final Method MethodHandles_lookup = ReflectionUtil.getDeclaredMethod(MethodHandles.class, "lookup");

      final RecordConsumerMethods RecordConsumer = new RecordConsumerMethods();
      final ByteBuddyMessageWritersMethods ByteBuddyProto3FastMessageWriter =
          new ByteBuddyMessageWritersMethods();
      final FieldWriterMethods FieldWriter = new FieldWriterMethods();

      static class RecordConsumerMethods {
        final Method startField =
            ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "startField", String.class, int.class);
        final Method endField =
            ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "endField", String.class, int.class);
        final Method startGroup = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "startGroup");
        final Method endGroup = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "endGroup");
        final Method addInteger =
            ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addInteger", int.class);
        final Method addLong = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addLong", long.class);
        final Method addBoolean =
            ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addBoolean", boolean.class);
        final Method addBinary =
            ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addBinary", Binary.class);
        final Method addFloat = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addFloat", float.class);
        final Method addDouble =
            ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addDouble", double.class);

        final Map<Class<?>, Method> PRIMITIVES = initPrimitives();

        private Map<Class<?>, Method> initPrimitives() {
          Map<Class<?>, Method> m = new HashMap<>();
          m.put(int.class, addInteger);
          m.put(long.class, addLong);
          m.put(boolean.class, addBoolean);
          m.put(float.class, addFloat);
          m.put(double.class, addDouble);
          return Collections.unmodifiableMap(m);
        }

        private RecordConsumerMethods() {}
      }

      static class ByteBuddyMessageWritersMethods {
        final Method getRecordConsumer = ReflectionUtil.getDeclaredMethod(
            WriteSupport.ByteBuddyMessageWriters.class, "getRecordConsumer");
        final Method enumNameNumberPairs = ReflectionUtil.getDeclaredMethod(
            WriteSupport.ByteBuddyMessageWriters.class, "enumNameNumberPairs", String.class);

        private ByteBuddyMessageWritersMethods() {}
      }

      static class FieldWriterMethods {
        final Method writeRawValue = ReflectionUtil.getDeclaredMethod(
            ProtoWriteSupport.FieldWriter.class, "writeRawValue", Object.class);
      }

      private ResolvedReflection() {}
    }

    static class Codegen {
      public static StackManipulation incIntVar(LocalVar var, int inc) {
        int offset = var.offset();
        return new StackManipulation.AbstractBase() {
          @Override
          public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
            methodVisitor.visitIincInsn(offset, inc);
            return Size.ZERO;
          }
        };
      }

      private static StackManipulation jumpTo(int jumpInst, Label label) {
        return new StackManipulation.AbstractBase() {
          @Override
          public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
            methodVisitor.visitJumpInsn(jumpInst, label);
            return Size.ZERO;
          }
        };
      }

      private static StackManipulation visitLabel(Label label) {
        return new StackManipulation.AbstractBase() {
          @Override
          public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
            methodVisitor.visitLabel(label);
            return Size.ZERO;
          }
        };
      }

      private static Implementation returnVoid() {
        return new Implementations(MethodReturn.VOID);
      }

      public static StackManipulation castLongToInt() {
        return castPrimitive(Opcodes.L2I);
      }

      public static StackManipulation castIntToLong() {
        return castPrimitive(Opcodes.I2L);
      }

      public static StackManipulation castPrimitive(int opcode) {
        return new StackManipulation.AbstractBase() {
          @Override
          public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
            methodVisitor.visitInsn(opcode);
            return Size.ZERO;
          }
        };
      }

      public static StackManipulation invokeMethod(Method method) {
        return MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(method));
      }

      public static StackManipulation invokeProtoMethod(
          Class<?> proto3MessageOrBuilderInterface,
          String name,
          Descriptors.FieldDescriptor fieldDescriptor,
          Class<?>... parameters) {
        return invokeMethod(ReflectionUtil.getDeclaredMethod(
            proto3MessageOrBuilderInterface, fieldDescriptor, name, parameters));
      }

      public static StackManipulation storeRecordConsumer(LocalVar recordConsumerVar) {
        return new StackManipulation.Compound(
            MethodVariableAccess.loadThis(),
            invokeMethod(Reflection.ByteBuddyProto3FastMessageWriter.getRecordConsumer),
            recordConsumerVar.store());
      }
    }

    static class Implementations implements Implementation {
      private final List<Implementation> implementations = new ArrayList<>();
      private final List<StackManipulation> ongoing = new ArrayList<>();

      private Implementation compound;

      public Implementations() {}

      public Implementations(StackManipulation... stackManipulations) {
        add(stackManipulations);
      }

      @Override
      public ByteCodeAppender appender(Target implementationTarget) {
        return compound.appender(implementationTarget);
      }

      @Override
      public InstrumentedType prepare(InstrumentedType instrumentedType) {
        if (compound != null) {
          throw new IllegalStateException();
        }
        flushOngoing();
        compound = new Compound(implementations);
        return compound.prepare(instrumentedType);
      }

      public Implementations add(Implementation... implementations) {
        flushOngoing();
        this.implementations.addAll(Arrays.asList(implementations));
        return this;
      }

      public Implementations add(ByteCodeAppender... appenders) {
        return add(new Simple(appenders));
      }

      public Implementations add(StackManipulation... stackManipulations) {
        ongoing.addAll(Arrays.asList(stackManipulations));
        return this;
      }

      private void flushOngoing() {
        if (!ongoing.isEmpty()) {
          implementations.add(new Simple(ongoing.toArray(new StackManipulation[0])));
          ongoing.clear();
        }
      }
    }

    static class LocalVar implements AutoCloseable {
      private final LocalVars vars;
      private final TypeDescription typeDescription;
      private final Class<?> clazz;
      private final int stackSize;

      private int refCount;

      private int offset;

      public LocalVar(Class<?> clazz, TypeDescription typeDescription, LocalVars vars) {
        this.clazz = clazz;
        this.typeDescription = typeDescription;
        this.vars = vars;
        this.stackSize = StackSize.of(typeDescription);
      }

      public LocalVars vars() {
        return vars;
      }

      public int offset() {
        assertRegistered();
        return offset;
      }

      public TypeDescription typeDescription() {
        return typeDescription;
      }

      public StackManipulation load() {
        return MethodVariableAccess.of(typeDescription()).loadFrom(offset());
      }

      public StackManipulation store() {
        return MethodVariableAccess.of(typeDescription()).storeAt(offset());
      }

      public Class<?> clazz() {
        if (clazz == null) {
          throw new IllegalStateException();
        }
        return clazz;
      }

      private int stackSize() {
        return stackSize;
      }

      public LocalVar register() {
        vars.register(this);
        return this;
      }

      public LocalVar alias() {
        assertRegistered();
        refCount += 1;
        return this;
      }

      public LocalVar unregister() {
        int index = assertRegistered();
        refCount -= 1;
        if (refCount == 0) {
          if (index != vars.vars.size() - 1) {
            throw new IllegalStateException("cannot deregister var " + this + "  from " + vars.vars);
          }
          vars.vars.remove(this);
        }
        return this;
      }

      private int assertRegistered() {
        int index = getIndex();
        if (index < 0) {
          throw new IllegalStateException("not registered");
        }
        return index;
      }

      private int getIndex() {
        return vars.vars.indexOf(this);
      }

      @Override
      public void close() {
        unregister();
      }

      @Override
      public String toString() {
        return "LocalVar{" + "vars="
            + vars + ", typeDescription="
            + typeDescription + ", stackSize="
            + stackSize + ", offset="
            + offset + '}';
      }
    }

    static class LocalVars {
      private final List<TypeDescription> frame = new ArrayList<>();
      private final List<LocalVar> vars = new ArrayList<>();
      private int maxSize;

      public LocalVar register(LocalVar var) {
        if (vars.contains(var)) {
          throw new IllegalStateException("cannot register var twice: " + var + ", " + vars);
        }
        int offset =
            vars.isEmpty() ? 0 : vars.get(vars.size() - 1).offset + vars.get(vars.size() - 1).stackSize;
        vars.add(var);
        var.offset = offset;
        var.refCount = 1;

        maxSize = Math.max(maxSize, getSize());
        return var;
      }

      public StackManipulation frameSame1(Class<?> varOnStack) {
        List<TypeDescription> currTypes = types();
        try {
          return new StackManipulation.AbstractBase() {
            @Override
            public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
              implementationContext
                  .getFrameGeneration()
                  .same1(methodVisitor, TypeDescription.ForLoadedType.of(varOnStack), currTypes);
              return Size.ZERO;
            }
          };
        } finally {
          this.frame.clear();
          this.frame.addAll(currTypes);
        }
      }

      public StackManipulation frameEmptyStack() {
        List<TypeDescription> currTypes = types();
        List<TypeDescription> frame = new ArrayList<>(this.frame);
        try {
          if (currTypes.size() < frame.size()) {
            int commonLength = commonTypesLength(currTypes, frame);
            if (commonLength < currTypes.size() || frame.size() - currTypes.size() > 3) {
              return new StackManipulation.AbstractBase() {
                @Override
                public Size apply(
                    MethodVisitor methodVisitor, Implementation.Context implementationContext) {
                  implementationContext
                      .getFrameGeneration()
                      .full(methodVisitor, Collections.emptyList(), currTypes);
                  return Size.ZERO;
                }
              };
            } else {
              return new StackManipulation.AbstractBase() {
                @Override
                public Size apply(
                    MethodVisitor methodVisitor, Implementation.Context implementationContext) {
                  implementationContext
                      .getFrameGeneration()
                      .chop(methodVisitor, frame.size() - currTypes.size(), currTypes);
                  return Size.ZERO;
                }
              };
            }
          } else if (currTypes.size() == frame.size()) {
            int commonLength = commonTypesLength(currTypes, frame);
            if (commonLength != currTypes.size()) {
              return new StackManipulation.AbstractBase() {
                @Override
                public Size apply(
                    MethodVisitor methodVisitor, Implementation.Context implementationContext) {
                  implementationContext
                      .getFrameGeneration()
                      .full(methodVisitor, Collections.emptyList(), currTypes);
                  return Size.ZERO;
                }
              };
            } else {
              return new StackManipulation.AbstractBase() {
                @Override
                public Size apply(
                    MethodVisitor methodVisitor, Implementation.Context implementationContext) {
                  implementationContext.getFrameGeneration().same(methodVisitor, currTypes);
                  return Size.ZERO;
                }
              };
            }
          } else {
            int commonLength = commonTypesLength(currTypes, frame);
            if (commonLength < frame.size() || currTypes.size() - frame.size() > 3) {
              return new StackManipulation.AbstractBase() {
                @Override
                public Size apply(
                    MethodVisitor methodVisitor, Implementation.Context implementationContext) {
                  implementationContext
                      .getFrameGeneration()
                      .full(methodVisitor, Collections.emptyList(), currTypes);
                  return Size.ZERO;
                }
              };
            } else {
              return new StackManipulation.AbstractBase() {
                @Override
                public Size apply(
                    MethodVisitor methodVisitor, Implementation.Context implementationContext) {
                  implementationContext
                      .getFrameGeneration()
                      .append(
                          methodVisitor,
                          currTypes.subList(frame.size(), currTypes.size()),
                          frame);
                  return Size.ZERO;
                }
              };
            }
          }
        } finally {
          this.frame.clear();
          this.frame.addAll(currTypes);
        }
      }

      private int commonTypesLength(List<TypeDescription> a, List<TypeDescription> b) {
        int len = Math.min(a.size(), b.size());
        for (int i = 0; i < len; i++) {
          if (!Objects.equals(a.get(i), b.get(i))) {
            return i;
          }
        }
        return len;
      }

      public LocalVar register(TypeDescription typeDescription) {
        LocalVar var = new LocalVar(null, typeDescription, this);
        return register(var);
      }

      public LocalVar register(Class<?> clazz) {
        LocalVar var = new LocalVar(clazz, TypeDescription.ForLoadedType.of(clazz), this);
        return register(var);
      }

      public Implementation asImplementation() {
        return new Implementation.Simple(new ByteCodeAppender() {
          @Override
          public Size apply(
              MethodVisitor methodVisitor,
              Implementation.Context implementationContext,
              MethodDescription instrumentedMethod) {
            return new Size(0, maxSize);
          }
        });
      }

      private int getSize() {
        int size = 0;
        for (LocalVar var : vars) {
          size += var.stackSize();
        }
        return size;
      }

      private List<TypeDescription> types() {
        List<TypeDescription> types = new ArrayList<>();
        for (LocalVar var : vars) {
          types.add(var.typeDescription);
        }
        return types;
      }
    }
  }

  static class ReflectionUtil {

    static Optional<? extends Class<? extends MessageOrBuilder>> getMessageOrBuilderInterface(
        Class<? extends Message> messageClass) {
      return Stream.of(messageClass)
          .filter(Objects::nonNull)
          .filter(ByteBuddyCodeGen::isGeneratedMessage)
          .flatMap(x -> Arrays.stream(x.getInterfaces()))
          .filter(MessageOrBuilder.class::isAssignableFrom)
          .map(x -> (Class<? extends MessageOrBuilder>) x)
          .findFirst();
    }

    static Method getDeclaredMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
      try {
        return clazz.getDeclaredMethod(name, parameterTypes);
      } catch (NoSuchMethodException e) {
        throw new CodeGenException(e);
      }
    }

    static Method getDeclaredMethod(
        Class<?> protoClazz, Descriptors.FieldDescriptor fieldDescriptor, String name, Class<?>... parameters) {
      return getDeclaredMethod(
          protoClazz, name.replace("{}", getFieldNameForMethod(fieldDescriptor)), parameters);
    }

    static Method getDeclaredMethodByName(Class<?> clazz, String name) {
      for (Method method : clazz.getDeclaredMethods()) {
        if (name.equals(method.getName())) {
          return method;
        }
      }
      throw new CodeGenException("no such method on class " + clazz + ": " + name);
    }

    static Method getDeclaredMethodByName(
        Class<?> clazz, Descriptors.FieldDescriptor fieldDescriptor, String name) {
      return getDeclaredMethodByName(clazz, name.replace("{}", getFieldNameForMethod(fieldDescriptor)));
    }

    // almost the same as com.google.protobuf.Descriptors.FieldDescriptor#fieldNameToJsonName
    // but capitalizing the first letter after each last digit
    static String getFieldNameForMethod(Descriptors.FieldDescriptor fieldDescriptor) {
      String name = fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.GROUP
          ? fieldDescriptor.getMessageType().getName()
          : fieldDescriptor.getName();
      final int length = name.length();
      StringBuilder result = new StringBuilder(length);
      boolean isNextUpperCase = false;
      for (int i = 0; i < length; i++) {
        char ch = name.charAt(i);
        if (ch == '_') {
          isNextUpperCase = true;
        } else if ('0' <= ch && ch <= '9') {
          isNextUpperCase = true;
          result.append(ch);
        } else if (isNextUpperCase || i == 0) {
          // This closely matches the logic for ASCII characters in:
          // http://google3/google/protobuf/descriptor.cc?l=249-251&rcl=228891689
          if ('a' <= ch && ch <= 'z') {
            ch = (char) (ch - 'a' + 'A');
          }
          result.append(ch);
          isNextUpperCase = false;
        } else {
          result.append(ch);
        }
      }
      return result.toString();
    }

    static <T> Constructor<T> getConstructor(Class<T> clazz, Class<?>... parameterTypes) {
      try {
        return clazz.getConstructor(parameterTypes);
      } catch (NoSuchMethodException e) {
        throw new CodeGenException(e);
      }
    }

    static <T> T newInstance(Constructor<T> constructor, Object... initParams) {
      try {
        return constructor.newInstance(initParams);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new CodeGenException(e);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof CodeGenException) {
          throw (CodeGenException) e.getCause();
        }
        throw new CodeGenException(e.getCause());
      }
    }

    static Optional<Class<?>> classForName(String className) {
      try {
        return Optional.of(Class.forName(className, false, ByteBuddyCodeGen.class.getClassLoader()));
      } catch (ClassNotFoundException e) {
        return Optional.empty();
      }
    }
  }

  public static class WriteSupport {
    // in order to avoid class generation for the same proto descriptors, cache implementations.
    private static final Map<MessageFieldsWritersCacheKey, Consumer<ProtoWriteSupport<?>.MessageWriter>>
        WRITERS_CACHE = new MapMaker().weakValues().makeMap();

    private static final Consumer<ProtoWriteSupport<?>.MessageWriter> NOOP_WRITER_PATCHER = messageWriter -> {};
    private static final Consumer<ProtoWriteSupport<?>.MessageWriter> REVERT_WRITER_PATCHER = messageWriter -> {
      Queue<ProtoWriteSupport<?>.FieldWriter> queue = new ArrayDeque<>();
      queue.add(messageWriter);

      while (!queue.isEmpty()) {
        ProtoWriteSupport<?>.FieldWriter fw = queue.poll();
        if (fw instanceof ProtoWriteSupport<?>.MessageWriter) {
          ((ProtoWriteSupport<?>.MessageWriter) fw)
              .setAlternativeMessageWriter(ProtoWriteSupport.MessageFieldsWriter.NOOP);
          queue.addAll(Arrays.asList(((ProtoWriteSupport<?>.MessageWriter) fw).fieldWriters));
        } else if (fw instanceof ProtoWriteSupport<?>.ArrayWriter) {
          queue.add(((ProtoWriteSupport<?>.ArrayWriter) fw).fieldWriter);
        } else if (fw instanceof ProtoWriteSupport<?>.RepeatedWriter) {
          queue.add(((ProtoWriteSupport<?>.RepeatedWriter) fw).fieldWriter);
        } else if (fw instanceof ProtoWriteSupport<?>.MapWriter) {
          queue.add(((ProtoWriteSupport<?>.MapWriter) fw).keyWriter);
          queue.add(((ProtoWriteSupport<?>.MapWriter) fw).valueWriter);
        }
      }
    };

    static class MessageFieldsWritersCacheKey {
      private final MessageType rootSchema;
      private final Class<? extends Message> protoMessage;
      private final boolean writeSpecsCompliant;
      private final boolean protoReflectionForExtendable;

      MessageFieldsWritersCacheKey(
          MessageType rootSchema,
          Class<? extends Message> protoMessage,
          boolean writeSpecsCompliant,
          boolean protoReflectionForExtendable) {
        this.rootSchema = rootSchema;
        this.protoMessage = protoMessage;
        this.writeSpecsCompliant = writeSpecsCompliant;
        this.protoReflectionForExtendable = protoReflectionForExtendable;
      }

      @Override
      public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        MessageFieldsWritersCacheKey that = (MessageFieldsWritersCacheKey) o;
        return writeSpecsCompliant == that.writeSpecsCompliant
            && protoReflectionForExtendable == that.protoReflectionForExtendable
            && Objects.equals(rootSchema, that.rootSchema)
            && Objects.equals(protoMessage, that.protoMessage);
      }

      @Override
      public int hashCode() {
        return Objects.hash(rootSchema, protoMessage, writeSpecsCompliant, protoReflectionForExtendable);
      }
    }

    static <T extends MessageOrBuilder> void tryApplyAlternativeMessageFieldsWriters(
        ProtoWriteSupport<T>.MessageWriter rootMessageWriter,
        MessageType rootSchema,
        Class<? extends Message> protoMessage,
        Descriptors.Descriptor descriptor,
        ProtoWriteSupport.CodegenMode codegenMode) {

      if (!codegenMode.tryCodeGen(protoMessage)) {
        return;
      }

      MessageFieldsWritersCacheKey cacheKey = new MessageFieldsWritersCacheKey(
          rootSchema,
          protoMessage,
          rootMessageWriter.getProtoWriteSupport().isWriteSpecsCompliant(),
          codegenMode.protobufReflectionForExtensions());

      try {
        Consumer<ProtoWriteSupport<?>.MessageWriter> messageFieldsWriterPatcher = WRITERS_CACHE.computeIfAbsent(
            cacheKey,
            unused -> createMessageFieldsWriterPatcher(
                rootMessageWriter, protoMessage, descriptor, codegenMode));
        messageFieldsWriterPatcher.accept(rootMessageWriter);
      } catch (Throwable t) {
        if (!codegenMode.ignoreCodeGenException()) {
          throw t;
        }
        REVERT_WRITER_PATCHER.accept(rootMessageWriter);
      }
    }

    private static Consumer<ProtoWriteSupport<?>.MessageWriter> createMessageFieldsWriterPatcher(
        ProtoWriteSupport<?>.MessageWriter rootMessageWriter,
        Class<? extends Message> protoMessage,
        Descriptors.Descriptor descriptor,
        ProtoWriteSupport.CodegenMode codegenMode) {
      return new ByteBuddyMessageWritersCodeGen(rootMessageWriter, protoMessage, descriptor, codegenMode)
          .getPatcher();
    }

    static class Field {
      private final FieldScanner fieldScanner;
      private final Field parent;
      private final ProtoWriteSupport<?>.FieldWriter fieldWriter;

      private final Descriptors.FieldDescriptor fieldDescriptor; // can be null for root MessageWriter
      private final Descriptors.Descriptor messageType; // filled for Message fields (incl. Map)

      private final String parquetFieldName;
      private final int parquetFieldIndex;

      private Type reflectionType;
      private Object codeGenerationBasicType;
      private Object codeGenerationKey;

      private List<Field> children;
      private Field mapKey;
      private Field mapValue;

      private Field(
          FieldScanner fieldScanner,
          Field parent,
          ProtoWriteSupport<?>.FieldWriter fieldWriter,
          Descriptors.FieldDescriptor fieldDescriptor,
          String parquetFieldName,
          int parquetFieldIndex) {
        this.fieldScanner = fieldScanner;
        this.parent = parent;
        this.fieldWriter = fieldWriter;
        this.fieldDescriptor = fieldDescriptor;
        this.messageType = fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
            ? fieldDescriptor.getMessageType()
            : null;
        this.parquetFieldName = parquetFieldName;
        this.parquetFieldIndex = parquetFieldIndex;
      }

      private Field(
          FieldScanner fieldScanner,
          ProtoWriteSupport<?>.MessageWriter messageWriter,
          Class<? extends Message> protoMessage,
          Descriptors.Descriptor messageType) {
        this.fieldScanner = fieldScanner;
        this.parent = null;
        this.fieldWriter = messageWriter;
        this.fieldDescriptor = null;
        this.messageType = messageType;
        this.reflectionType = protoMessage;
        this.parquetFieldName = null;
        this.parquetFieldIndex = -1;
      }

      public String getParquetFieldName() {
        return parquetFieldName;
      }

      public int getParquetFieldIndex() {
        return parquetFieldIndex;
      }

      public Field getParent() {
        return parent;
      }

      @Override
      public String toString() {
        List<String> path = new ArrayList<>();
        Field p = this;
        while (p != null) {
          path.add(p.getParquetFieldName());
          p = p.getParent();
        }
        Collections.reverse(path);
        return String.valueOf(path);
      }

      public Descriptors.Descriptor getMessageType() {
        return messageType;
      }

      // helps codegen to deal with particular java getter for a proto field
      public Type getReflectionType() {
        if (reflectionType == null) {
          reflectionType = initReflectionType();
        }
        return reflectionType;
      }

      public Class<? extends MessageOrBuilder> getMessageOrBuilderInterface() {
        if (!isProtoMessage()) {
          throw new CodeGenException();
        }
        return ReflectionUtil.getMessageOrBuilderInterface((Class<? extends Message>) getReflectionType())
            .get();
      }

      public boolean isList() {
        return !isMap() && fieldDescriptor != null && fieldDescriptor.isRepeated();
      }

      private Type initReflectionType() {
        // parent is always not null here
        if (isMap()) {
          return initMapReflectionType();
        } else if (parent.isMap()) {
          MapReflectionType mapReflectionType = (MapReflectionType) parent.getReflectionType();
          return fieldDescriptor.getIndex() == 0 ? mapReflectionType.key() : mapReflectionType.value();
        } else {
          return initRegularFieldReflectionType();
        }
      }

      private Type initRegularFieldReflectionType() {
        Class<?> clazz;
        Class<?> parentProtoMessage = (Class<?>) parent.getReflectionType();
        if (fieldDescriptor.isRepeated()) {
          clazz = ReflectionUtil.getDeclaredMethod(parentProtoMessage, fieldDescriptor, "get{}", int.class)
              .getReturnType();
        } else {
          clazz = ReflectionUtil.getDeclaredMethod(parentProtoMessage, fieldDescriptor, "get{}")
              .getReturnType();
        }
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
          return new EnumReflectionType(clazz, fieldDescriptor);
        }
        return clazz;
      }

      private Type initMapReflectionType() {
        Class<?> parentProtoMessage = (Class<?>) parent.getReflectionType();
        Method method =
            ReflectionUtil.getDeclaredMethodByName(parentProtoMessage, fieldDescriptor, "get{}OrThrow");
        Descriptors.FieldDescriptor valueFieldDescriptor =
            fieldDescriptor.getMessageType().getFields().get(1);
        Type valueType;
        if (valueFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
          valueType = new EnumReflectionType(method.getReturnType(), valueFieldDescriptor);
        } else {
          valueType = method.getReturnType();
        }
        return new MapReflectionType(method.getParameterTypes()[0], valueType);
      }

      // helps codegen to identify unique methods and supporting fields to write messages, map entries and enums
      public Object getCodeGenerationElementKey() {
        if (codeGenerationKey == null) {
          codeGenerationKey = initCodeGenerationKey();
        }
        return codeGenerationKey;
      }

      private Object initCodeGenerationKey() {
        if (isMessage() || (isMap() && mapValue().isMessage())) {
          List<Object> key = new ArrayList<>();
          key.add(getCodeGenerationBasicType());
          for (Field child : getChildren()) {
            if (child.isProtoMessage()
                || (child.isMap() && child.mapValue().isProtoMessage())) {
              key.add(child.getCodeGenerationElementKey());
            }
          }
          return key;
        }
        if (isBinaryMessage() || (isMap() && mapValue().isBinaryMessage())) {
          return getCodeGenerationBasicType();
        }
        if (isMap()) {
          return getCodeGenerationBasicType();
        }
        if (isEnum()) {
          // for enums extra fields have to be prepared and their content depend on Enum type itself, not on
          // the declaring message type
          return getFieldDescriptor().getEnumType();
        }
        throw new CodeGenException("no code generation is allowed for this field");
      }

      private Object getCodeGenerationBasicType() {
        if (codeGenerationBasicType == null) {
          codeGenerationBasicType = initCodeGenerationBasicType();
        }
        return codeGenerationBasicType;
      }

      private Object initCodeGenerationBasicType() {
        if (isMap()) {
          Object keyType = mapKey().getCodeGenerationBasicType();
          Object valueType = mapValue().getCodeGenerationBasicType();
          return Arrays.asList(keyType, valueType);
        } else if (isProtoMessage()) {
          return Arrays.asList(isBinaryMessage() ? "binary_message" : "message", getMessageType());
        } else if (isEnum()) {
          return Arrays.asList(
              getFieldDescriptor().getEnumType(),
              getFieldDescriptor().legacyEnumFieldTreatedAsClosed());
        } else {
          return getFieldDescriptor().getJavaType();
        }
      }

      public Descriptors.FieldDescriptor getFieldDescriptor() {
        return fieldDescriptor;
      }

      public List<Field> getChildren() {
        if (children == null) {
          children = initChildren();
        }
        return children;
      }

      private List<Field> initChildren() {
        if (isMessage()) {
          ProtoWriteSupport<?>.FieldWriter[] fieldWriters = getMessageWriter().fieldWriters;
          return resolveChildFields(fieldWriters);
        } else if (isMap()) {
          return Arrays.asList(mapKey(), mapValue());
        } else {
          return Collections.emptyList();
        }
      }

      private List<Field> resolveChildFields(ProtoWriteSupport<?>.FieldWriter[] fieldWriters) {
        List<Descriptors.FieldDescriptor> fieldDescriptors = messageType.getFields();
        int fieldsCount = fieldWriters.length;
        List<Field> result = new ArrayList<>(fieldsCount);
        for (int i = 0; i < fieldsCount; i++) {
          result.add(resolveField(fieldWriters[i], fieldDescriptors.get(i)));
        }
        return result;
      }

      public boolean isMessage() {
        // this does not include Map and Message fields written as binary
        return isProtoMessage() && fieldWriter instanceof ProtoWriteSupport<?>.MessageWriter;
      }

      public boolean isBinaryMessage() {
        return isProtoMessage() && fieldWriter instanceof ProtoWriteSupport<?>.BinaryWriter;
      }

      public boolean isProtoMessage() {
        return !isMap() && !isProtoWrapper() && messageType != null;
      }

      private ProtoWriteSupport<?>.MessageWriter getMessageWriter() {
        if (!isMessage()) {
          throw new CodeGenException();
        }
        return (ProtoWriteSupport<?>.MessageWriter) fieldWriter;
      }

      public boolean isFieldWriterFallbackTransition() {
        // track only those 'protobuf reflection writers that are children of codegen writers'
        Field parent = getParent();
        while (parent != null) {
          if (parent.isMessage()) {
            break;
          }
          parent = parent.getParent();
        }

        return (parent != null && !parent.isFieldWriterFallbackTransition() && isFieldWriterFallback())
            || (parent == null && isFieldWriterFallback());
      }

      private boolean isFieldWriterFallback() {
        if (isBinaryMessage()) return true;
        if (isMessage() && fieldScanner.isFieldWriterFallbackForExtendable() && isExtendableMessage())
          return true;
        return false;
      }

      private boolean isExtendableMessage() {
        if (!isMessage()) {
          throw new CodeGenException();
        }
        Class<? extends Message> protoMessage = (Class<? extends Message>) getReflectionType();
        return ByteBuddyCodeGen.isExtendableMessage(protoMessage);
      }

      public boolean isMap() {
        // fieldDescriptor is null for root message which is message, not map.
        return fieldDescriptor != null && fieldDescriptor.isMapField();
      }

      private Field mapKey() {
        if (mapKey == null) {
          mapKey = initMapKey();
        }
        return mapKey;
      }

      private Field initMapKey() {
        if (!isMap()) {
          throw new CodeGenException();
        }
        if (fieldWriter instanceof ProtoWriteSupport<?>.MessageWriter) {
          return resolveField(
              ((ProtoWriteSupport<?>.MessageWriter) fieldWriter).fieldWriters[0],
              messageType.getFields().get(0));
        } else if (fieldWriter instanceof ProtoWriteSupport<?>.MapWriter) {
          return resolveField(
              ((ProtoWriteSupport<?>.MapWriter) fieldWriter).keyWriter,
              messageType.getFields().get(0));
        } else {
          throw new CodeGenException();
        }
      }

      private Field mapValue() {
        if (mapValue == null) {
          mapValue = initMapValue();
        }
        return mapValue;
      }

      private Field initMapValue() {
        if (!isMap()) {
          throw new CodeGenException();
        }
        if (fieldWriter instanceof ProtoWriteSupport<?>.MessageWriter) {
          return resolveField(
              ((ProtoWriteSupport<?>.MessageWriter) fieldWriter).fieldWriters[1],
              messageType.getFields().get(1));
        } else if (fieldWriter instanceof ProtoWriteSupport<?>.MapWriter) {
          return resolveField(
              ((ProtoWriteSupport<?>.MapWriter) fieldWriter).valueWriter,
              messageType.getFields().get(1));
        } else {
          throw new CodeGenException();
        }
      }

      public boolean isEnum() {
        return fieldWriter instanceof ProtoWriteSupport<?>.EnumWriter;
      }

      private Field resolveField(
          ProtoWriteSupport<?>.FieldWriter fieldWriter, Descriptors.FieldDescriptor fieldDescriptor) {
        return resolveField(fieldWriter, fieldDescriptor, fieldWriter);
      }

      private Field resolveField(
          ProtoWriteSupport<?>.FieldWriter fieldWriter,
          Descriptors.FieldDescriptor fieldDescriptor,
          ProtoWriteSupport<?>.FieldWriter parquetFieldInfo) {
        if (fieldWriter instanceof ProtoWriteSupport<?>.ArrayWriter) {
          return resolveField(
              ((ProtoWriteSupport<?>.ArrayWriter) fieldWriter).fieldWriter, fieldDescriptor, fieldWriter);
        } else if (fieldWriter instanceof ProtoWriteSupport<?>.RepeatedWriter) {
          return resolveField(
              ((ProtoWriteSupport<?>.RepeatedWriter) fieldWriter).fieldWriter,
              fieldDescriptor,
              fieldWriter);
        } else {
          if (!Objects.equals(parquetFieldInfo.fieldName, fieldDescriptor.getName())) {
            throw new CodeGenException("fields mismatch: parquetFieldInfo: " + parquetFieldInfo.fieldName
                + ", fieldDescriptor: " + fieldDescriptor);
          }
          return new Field(
              fieldScanner,
              this,
              fieldWriter,
              fieldDescriptor,
              parquetFieldInfo.fieldName,
              parquetFieldInfo.index);
        }
      }

      public boolean isOptional() {
        return !isMap() && !isList() && fieldDescriptor != null && fieldDescriptor.hasPresence();
      }

      public boolean isPrimitive() {
        switch (fieldDescriptor.getJavaType()) {
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case BOOLEAN:
            return true;
          default:
            return false;
        }
      }

      public boolean isBinary() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.BYTE_STRING;
      }

      public boolean isString() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING;
      }

      public boolean isProtoWrapper() {
        return fieldWriter instanceof ProtoWriteSupport<?>.BytesValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.StringValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.BoolValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.UInt32ValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.Int32ValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.UInt64ValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.Int64ValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.FloatValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.DoubleValueWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.TimeWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.DateWriter
            || fieldWriter instanceof ProtoWriteSupport<?>.TimestampWriter;
      }
    }

    static final class MapReflectionType implements Type {
      private final Type key;
      private final Type value;

      public MapReflectionType(Type key, Type value) {
        this.key = key;
        this.value = value;
      }

      public Type key() {
        return key;
      }

      public Type value() {
        return value;
      }

      @Override
      public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        MapReflectionType that = (MapReflectionType) o;
        return Objects.equals(key, that.key) && Objects.equals(value, that.value);
      }

      @Override
      public int hashCode() {
        return Objects.hash(key, value);
      }

      @Override
      public String toString() {
        return "MapReflectionType{" + "key=" + key + ", value=" + value + '}';
      }
    }

    static final class EnumReflectionType implements Type {
      private final Class<?> clazz;
      private final boolean enumSupportsUnknownValues; // determines if Enum actually supports unknown values
      private final boolean
          fieldSupportsUnknownValues; // only used to help identify which getter to use for enums

      public EnumReflectionType(Class<?> clazz, Descriptors.FieldDescriptor enumField) {
        this.clazz = clazz;
        this.enumSupportsUnknownValues = !enumField.getEnumType().isClosed();
        this.fieldSupportsUnknownValues = !enumField.legacyEnumFieldTreatedAsClosed();
      }

      @Override
      public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        EnumReflectionType that = (EnumReflectionType) o;
        return enumSupportsUnknownValues == that.enumSupportsUnknownValues
            && fieldSupportsUnknownValues == that.fieldSupportsUnknownValues
            && Objects.equals(clazz, that.clazz);
      }

      @Override
      public int hashCode() {
        return Objects.hash(clazz, enumSupportsUnknownValues, fieldSupportsUnknownValues);
      }

      @Override
      public String toString() {
        return "EnumReflectionType{" + "clazz="
            + clazz + ", enumSupportsUnknownValues="
            + enumSupportsUnknownValues + ", fieldSupportsUnknownValues="
            + fieldSupportsUnknownValues + '}';
      }
    }

    interface FieldVisitor {
      void visitField(Field field);
    }

    static class FieldScanner {
      private final boolean fieldWriterFallbackForExtendable;

      private FieldScanner(boolean fieldWriterFallbackForExtendable) {
        this.fieldWriterFallbackForExtendable = fieldWriterFallbackForExtendable;
      }

      public boolean isFieldWriterFallbackForExtendable() {
        return fieldWriterFallbackForExtendable;
      }

      public void scan(
          ProtoWriteSupport<?>.MessageWriter messageWriter,
          Class<? extends Message> protoMessage,
          Descriptors.Descriptor messageType,
          FieldVisitor visitor) {
        scan(new Field(this, messageWriter, protoMessage, messageType), visitor);
      }

      public void scan(Field startField, FieldVisitor visitor) {
        Queue<Field> queue = new ArrayDeque<>();
        queue.add(startField);

        while (!queue.isEmpty()) {
          Field field = queue.poll();
          visitor.visitField(field);
          queue.addAll(field.getChildren());
        }
      }
    }

    static class ByteBuddyMessageWritersCodeGen {
      private final FieldScanner fieldScanner;
      private final Class<? extends Message> protoMessage;
      private final Descriptors.Descriptor descriptor;
      private final ProtoWriteSupport<?> protoWriteSupport;

      private final Map<Object, CodeGenMessageWriter> codeGenMessageWriters = new LinkedHashMap<>();
      private final Map<Object, CodeGenMapWriter> mapWriters = new LinkedHashMap<>();
      private final Map<Object, CodeGenFieldWriterFallback> fieldWriterFallbacks = new LinkedHashMap<>();
      private final Map<Object, CodeGenEnum> enumFields = new LinkedHashMap<>();

      private DynamicType.Builder<ByteBuddyMessageWriters> classBuilder;
      private final Class<? extends ByteBuddyMessageWriters> byteBuddyMessageWritersClass;

      public ByteBuddyMessageWritersCodeGen(
          ProtoWriteSupport<?>.MessageWriter messageWriter,
          Class<? extends Message> protoMessage,
          Descriptors.Descriptor descriptor,
          ProtoWriteSupport.CodegenMode codegenMode) {
        this.protoWriteSupport = messageWriter.getProtoWriteSupport();
        this.fieldScanner = new FieldScanner(codegenMode.protobufReflectionForExtensions());
        this.protoMessage = protoMessage;
        this.descriptor = descriptor;

        collectCodeGenElements(messageWriter, protoMessage, descriptor);

        if (mapWriters.isEmpty() && codeGenMessageWriters.isEmpty()) {
          byteBuddyMessageWritersClass = null;
          return;
        }

        classBuilder = new ByteBuddy()
            .subclass(ByteBuddyMessageWriters.class)
            .modifiers(Visibility.PUBLIC)
            .name(ByteBuddyMessageWriters.class.getName() + "$Generated$"
                + BYTE_BUDDY_CLASS_SEQUENCE.incrementAndGet());

        registerEnumFields();
        registerFallbackFieldWriterFields();
        generateConstructor();
        generateMethods();
        overrideSetFallbackFieldWriters();
        overrideGetLookup();

        DynamicType.Unloaded<ByteBuddyMessageWriters> unloaded = classBuilder.make();

        //         use to debug codegen
        //         try {
        //           unloaded.saveIn(new java.io.File("generated_debug"));
        //         } catch (Exception e) {
        //         }

        byteBuddyMessageWritersClass = unloaded.load(
                this.getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
            .getLoaded();
      }

      private void overrideGetLookup() {
        classBuilder = classBuilder
            .method(ElementMatchers.named("getLookup"))
            .intercept(MethodCall.invoke(Reflection.MethodHandles_lookup));
      }

      private void registerFallbackFieldWriterFields() {
        for (CodeGenFieldWriterFallback fieldWriterFallback : fieldWriterFallbacks.values()) {
          classBuilder = classBuilder.define(fieldWriterFallback.fieldWriter());
        }
      }

      private void overrideSetFallbackFieldWriters() {
        if (fieldWriterFallbacks.isEmpty()) {
          return;
        }
        classBuilder = classBuilder
            .method(ElementMatchers.named("setFallbackFieldWriters"))
            .intercept(new Implementations() {
              {
                CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                try (LocalVar thisLocalVar = localVars.register(classBuilder.toTypeDescription())) {
                  try (LocalVar fieldWriters =
                      localVars.register(ProtoWriteSupport.FieldWriter[].class)) {
                    for (CodeGenFieldWriterFallback fieldWriterFallback :
                        fieldWriterFallbacks.values()) {
                      add(
                          MethodVariableAccess.loadThis(),
                          fieldWriters.load(),
                          IntegerConstant.forValue(fieldWriterFallback.getId()),
                          ArrayAccess.REFERENCE.load(),
                          FieldAccess.forField(fieldWriterFallback.fieldWriter())
                              .write());
                    }
                  }
                }

                add(Codegen.returnVoid());
              }
            });
      }

      private void generateMethods() {
        for (CodeGenMessageWriter codeGenMessageWriter : codeGenMessageWriters.values()) {
          classBuilder = classBuilder
              .define(codeGenMessageWriter.getMethodDescription())
              .intercept(new WriteAllFieldsForMessageImplementation(codeGenMessageWriter.getField()));
        }

        for (CodeGenMapWriter codeGenMapWriter : mapWriters.values()) {
          classBuilder = classBuilder
              .define(codeGenMapWriter.writeMapEntry())
              .intercept(new WriteAllFieldsForMapEntryImplementation(codeGenMapWriter.getField()));
        }
      }

      private void generateConstructor() {
        classBuilder = classBuilder
            .constructor(ElementMatchers.any())
            .intercept(SuperMethodCall.INSTANCE.andThen(new ByteBuddyMessageWritersConstructor()))
            .modifiers(Visibility.PUBLIC);
      }

      private void registerEnumFields() {
        for (CodeGenEnum enumField : enumFields.values()) {
          classBuilder = classBuilder.define(enumField.enumNumberPairs());
          classBuilder = classBuilder.define(enumField.enumDescriptor());
          classBuilder = classBuilder.define(enumField.enumValues());
        }
      }

      private void collectCodeGenElements(
          ProtoWriteSupport<?>.MessageWriter messageWriter,
          Class<? extends Message> protoMessage,
          Descriptors.Descriptor descriptor) {
        fieldScanner.scan(messageWriter, protoMessage, descriptor, new FieldVisitor() {
          @Override
          public void visitField(Field field) {
            if (field.isFieldWriterFallback()) {
              if (field.isFieldWriterFallbackTransition()) {
                addCodeGenElement(field, fieldWriterFallbacks, CodeGenFieldWriterFallback::new);
              }
            } else if (field.isMessage()) {
              addCodeGenElement(field, codeGenMessageWriters, CodeGenMessageWriter::new);
            } else if (field.isMap()) {
              addCodeGenElement(field, mapWriters, CodeGenMapWriter::new);
            } else if (field.isEnum()) {
              addCodeGenElement(field, enumFields, CodeGenEnum::new);
            }
          }
        });
      }

      private class ByteBuddyMessageWritersConstructor extends Implementations {

        public ByteBuddyMessageWritersConstructor() {
          // final Map<String, Integer> enumNameNumberPairs<idx>;
          // final Descriptors.EnumDescriptor enumDescriptor<idx>;
          // final List<Descriptors.EnumValueDescriptor> enumValues<idx>;

          for (CodeGenEnum enumField : enumFields.values()) {
            add(
                MethodVariableAccess.loadThis(),
                MethodVariableAccess.loadThis(),
                new TextConstant(enumField.getEnumTypeFullName()),
                Codegen.invokeMethod(Reflection.ByteBuddyProto3FastMessageWriter.enumNameNumberPairs),
                FieldAccess.forField(enumField.enumNumberPairs())
                    .write());

            add(
                MethodVariableAccess.loadThis(),
                Codegen.invokeMethod(
                    ReflectionUtil.getDeclaredMethod(enumField.getEnumClass(), "getDescriptor")),
                FieldAccess.forField(enumField.enumDescriptor()).write());

            add(
                MethodVariableAccess.loadThis(),
                MethodVariableAccess.loadThis(),
                FieldAccess.forField(enumField.enumDescriptor()).read(),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                    Descriptors.EnumDescriptor.class, "getValues")),
                ArrayFactory.forType(TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(
                        Descriptors.EnumValueDescriptor.class))
                    .withValues(Collections.emptyList()),
                Codegen.invokeMethod(
                    ReflectionUtil.getDeclaredMethod(List.class, "toArray", Object[].class)),
                TypeCasting.to(
                    TypeDescription.ForLoadedType.of(Descriptors.EnumValueDescriptor[].class)),
                FieldAccess.forField(enumField.enumValues()).write());
          }

          add(Codegen.returnVoid());
        }
      }

      private abstract class FastMessageWriterMethodBase extends Implementations {
        final CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
          add(localVars.asImplementation());
          return super.prepare(instrumentedType);
        }

        abstract class RegularFieldWriterTemplate extends Implementations {
          final Field field;
          final LocalVar recordConsumerVar;

          RegularFieldWriterTemplate(Field field, LocalVar recordConsumerVar) {
            this.field = field;
            this.recordConsumerVar = recordConsumerVar;
          }

          String getterMethodTemplate() {
            return "get{}";
          }

          Implementation fieldGetConvertWrite(LocalVar proto3MessageOrBuilder) {
            if (field.isList()) {
              Label afterIfCountGreaterThanZero = new Label();
              try (LocalVar countVar = localVars.register(int.class)) {
                add(
                    proto3MessageOrBuilder.load(),
                    Codegen.invokeProtoMethod(
                        proto3MessageOrBuilder.clazz(),
                        "get{}Count",
                        field.getFieldDescriptor()),
                    countVar.store(),
                    countVar.load(),
                    Codegen.jumpTo(Opcodes.IFLE, afterIfCountGreaterThanZero),
                    recordConsumerVar.load(),
                    new TextConstant(field.getParquetFieldName()),
                    IntegerConstant.forValue(field.getParquetFieldIndex()),
                    Codegen.invokeMethod(Reflection.RecordConsumer.startField));

                if (protoWriteSupport.isWriteSpecsCompliant()) {
                  add(
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.startGroup),
                      recordConsumerVar.load(),
                      new TextConstant("list"),
                      IntegerConstant.forValue(0),
                      Codegen.invokeMethod(Reflection.RecordConsumer.startField));
                }

                Label nextIteration = new Label();
                Label afterForLoop = new Label();
                try (LocalVar iVar = localVars.register(int.class)) {
                  add(
                      IntegerConstant.forValue(0),
                      iVar.store(),
                      Codegen.visitLabel(nextIteration),
                      localVars.frameEmptyStack(),
                      iVar.load(),
                      countVar.load(),
                      Codegen.jumpTo(Opcodes.IF_ICMPGE, afterForLoop));

                  if (protoWriteSupport.isWriteSpecsCompliant()) {
                    add(
                        recordConsumerVar.load(),
                        Codegen.invokeMethod(Reflection.RecordConsumer.startGroup),
                        recordConsumerVar.load(),
                        new TextConstant("element"),
                        IntegerConstant.forValue(0),
                        Codegen.invokeMethod(Reflection.RecordConsumer.startField));
                  }

                  writeRepeatedRawValue(proto3MessageOrBuilder, iVar);

                  if (protoWriteSupport.isWriteSpecsCompliant()) {
                    add(
                        recordConsumerVar.load(),
                        new TextConstant("element"),
                        IntegerConstant.forValue(0),
                        Codegen.invokeMethod(Reflection.RecordConsumer.endField),
                        recordConsumerVar.load(),
                        Codegen.invokeMethod(Reflection.RecordConsumer.endGroup));
                  }

                  add(Codegen.incIntVar(iVar, 1), Codegen.jumpTo(Opcodes.GOTO, nextIteration));
                }

                add(Codegen.visitLabel(afterForLoop), localVars.frameEmptyStack());

                if (protoWriteSupport.isWriteSpecsCompliant()) {
                  add(
                      recordConsumerVar.load(),
                      new TextConstant("list"),
                      IntegerConstant.forValue(0),
                      Codegen.invokeMethod(Reflection.RecordConsumer.endField),
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.endGroup));
                }

                add(
                    recordConsumerVar.load(),
                    new TextConstant(field.getParquetFieldName()),
                    IntegerConstant.forValue(field.getParquetFieldIndex()),
                    Codegen.invokeMethod(Reflection.RecordConsumer.endField));
              }
              add(Codegen.visitLabel(afterIfCountGreaterThanZero), localVars.frameEmptyStack());
            } else {
              Label afterEndField = new Label();
              if (field.isOptional()) {
                add(
                    proto3MessageOrBuilder.load(),
                    Codegen.invokeProtoMethod(
                        proto3MessageOrBuilder.clazz(), "has{}", field.getFieldDescriptor()),
                    Codegen.jumpTo(Opcodes.IFEQ, afterEndField));
              }

              add(
                  recordConsumerVar.load(),
                  new TextConstant(field.fieldWriter.fieldName),
                  IntegerConstant.forValue(field.fieldWriter.index),
                  Codegen.invokeMethod(Reflection.RecordConsumer.startField));

              writeRawValue(proto3MessageOrBuilder);

              add(
                  recordConsumerVar.load(),
                  new TextConstant(field.fieldWriter.fieldName),
                  IntegerConstant.forValue(field.fieldWriter.index),
                  Codegen.invokeMethod(Reflection.RecordConsumer.endField));

              if (field.isOptional()) {
                add(Codegen.visitLabel(afterEndField), localVars.frameEmptyStack());
              }
            }

            return this;
          }

          void loadRepeatedValueOnStack(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            add(
                proto3MessageOrBuilder.load(),
                iVar.load(),
                Codegen.invokeProtoMethod(
                    proto3MessageOrBuilder.clazz(),
                    getterMethodTemplate(),
                    field.getFieldDescriptor(),
                    int.class));
          }

          void writeRepeatedRawValue(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            beforeLoadValueOnStack();
            loadRepeatedValueOnStack(proto3MessageOrBuilder, iVar);
            convertRawValueAndWrite();
            afterConvertRawValue();
          }

          Implementation writeFromVar(LocalVar var) {
            add(
                recordConsumerVar.load(),
                new TextConstant(field.getParquetFieldName()),
                IntegerConstant.forValue(field.getParquetFieldIndex()),
                Codegen.invokeMethod(Reflection.RecordConsumer.startField));
            beforeLoadValueOnStack();
            add(var.load());
            convertRawValueAndWrite();
            afterConvertRawValue();
            add(
                recordConsumerVar.load(),
                new TextConstant(field.getParquetFieldName()),
                IntegerConstant.forValue(field.getParquetFieldIndex()),
                Codegen.invokeMethod(Reflection.RecordConsumer.endField));
            return this;
          }

          void loadSingleValueOnStack(LocalVar proto3MessageOrBuilder) {
            add(
                proto3MessageOrBuilder.load(),
                Codegen.invokeProtoMethod(
                    proto3MessageOrBuilder.clazz(),
                    getterMethodTemplate(),
                    field.getFieldDescriptor()));
          }

          void beforeLoadValueOnStack() {
            add(recordConsumerVar.load());
          }

          void afterConvertRawValue() {}

          void writeRawValue(LocalVar proto3MessageOrBuilder) {
            beforeLoadValueOnStack();
            loadSingleValueOnStack(proto3MessageOrBuilder);
            convertRawValueAndWrite();
            afterConvertRawValue();
          }

          abstract void convertRawValueAndWrite();
        }

        private Implementation writePrimitiveField(
            LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, Field field) {
          return new PrimitiveFieldWriter(field, recordConsumerVar)
              .fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeBinaryField(
            LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, Field field) {
          return new BinaryFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeStringField(
            LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, Field field) {
          return new StringFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeProtoWrapperField(
            LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, Field field) {
          return new ProtoWrapperFieldWriter(field, recordConsumerVar)
              .fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeEnumField(
            LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, Field field) {
          return new EnumFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeMessageField(
            LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, Field field) {
          return new MessageFieldWriter(field, recordConsumerVar)
              .fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        class PrimitiveFieldWriter extends RegularFieldWriterTemplate {
          public PrimitiveFieldWriter(Field field, LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          @Override
          void convertRawValueAndWrite() {
            add(Codegen.invokeMethod(
                Reflection.RecordConsumer.PRIMITIVES.get((Class<?>) field.getReflectionType())));
          }
        }

        class MessageFieldWriter extends RegularFieldWriterTemplate {

          MessageFieldWriter(Field field, LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          String getterMethodTemplate() {
            return "get{}OrBuilder";
          }

          @Override
          void convertRawValueAndWrite() {
            if (!field.isFieldWriterFallbackTransition()) {
              CodeGenMessageWriter codeGenMessageWriter =
                  codeGenMessageWriters.get(field.getCodeGenerationElementKey());
              if (codeGenMessageWriter == null) {
                throw new CodeGenException("field: " + field);
              }
              MethodDescription methodDescription = codeGenMessageWriter.getMethodDescription();
              add(MethodInvocation.invoke(methodDescription));
            } else {
              add(Codegen.invokeMethod(Reflection.FieldWriter.writeRawValue));
            }
          }

          @Override
          void loadRepeatedValueOnStack(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            add(
                proto3MessageOrBuilder.load(),
                iVar.load(),
                Codegen.invokeProtoMethod(
                    proto3MessageOrBuilder.clazz(),
                    getterMethodTemplate(),
                    field.getFieldDescriptor(),
                    int.class));
          }

          @Override
          void beforeLoadValueOnStack() {
            if (!field.isFieldWriterFallbackTransition()) {
              startGroup();
              add(MethodVariableAccess.loadThis());
            } else {
              add(
                  MethodVariableAccess.loadThis(),
                  FieldAccess.forField(fieldWriterFallbacks
                          .get(field.getCodeGenerationElementKey())
                          .fieldWriter())
                      .read());
            }
          }

          @Override
          void loadSingleValueOnStack(LocalVar proto3MessageOrBuilder) {
            add(
                proto3MessageOrBuilder.load(),
                Codegen.invokeProtoMethod(
                    proto3MessageOrBuilder.clazz(),
                    getterMethodTemplate(),
                    field.getFieldDescriptor()));
          }

          @Override
          void afterConvertRawValue() {
            if (!field.isFieldWriterFallbackTransition()) {
              endGroup();
            }
          }

          void startGroup() {
            add(recordConsumerVar.load(), Codegen.invokeMethod(Reflection.RecordConsumer.startGroup));
          }

          void endGroup() {
            add(recordConsumerVar.load(), Codegen.invokeMethod(Reflection.RecordConsumer.endGroup));
          }

          Implementation writeMessageFieldsInternal(LocalVar proto3MessageOrBuilder) {

            if (!field.isFieldWriterFallbackTransition()) {
              for (Field child : field.getChildren()) {
                if (child.isProtoMessage()) {
                  add(writeMessageField(proto3MessageOrBuilder, recordConsumerVar, child));
                } else if (child.isMap()) {
                  add(writeMapField(child, proto3MessageOrBuilder));
                } else {
                  add(writeNonMessageRegularField(child, proto3MessageOrBuilder));
                }
              }
            } else {
              add(
                  MethodVariableAccess.loadThis(),
                  FieldAccess.forField(fieldWriterFallbacks
                          .get(field.getCodeGenerationElementKey())
                          .fieldWriter())
                      .read(),
                  proto3MessageOrBuilder.load(),
                  Codegen.invokeMethod(Reflection.FieldWriter.writeRawValue));
            }

            return this;
          }

          private Implementation writeMapField(Field field, LocalVar proto3MessageOrBuilder) {
            return new Implementations() {
              {
                CodeGenMapWriter codeGenMapWriter = mapWriters.get(field.getCodeGenerationElementKey());
                MethodDescription methodDescription = codeGenMapWriter.writeMapEntry();

                Class<?>[] parameters = codeGenMapWriter.writeMapEntryParameters();

                TypeDescription keyType = TypeDescription.ForLoadedType.of(parameters[0])
                    .asBoxed();
                TypeDescription valueType = TypeDescription.ForLoadedType.of(parameters[1])
                    .asBoxed();

                Label after = new Label();
                add(
                    proto3MessageOrBuilder.load(),
                    Codegen.invokeProtoMethod(
                        proto3MessageOrBuilder.clazz(),
                        "get{}Count",
                        field.getFieldDescriptor()),
                    Codegen.jumpTo(Opcodes.IFLE, after),
                    recordConsumerVar.load(),
                    new TextConstant(field.getParquetFieldName()),
                    IntegerConstant.forValue(field.getParquetFieldIndex()),
                    Codegen.invokeMethod(Reflection.RecordConsumer.startField));

                if (protoWriteSupport.isWriteSpecsCompliant()) {
                  add(
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.startGroup),
                      recordConsumerVar.load(),
                      new TextConstant("key_value"),
                      IntegerConstant.forValue(0),
                      Codegen.invokeMethod(Reflection.RecordConsumer.startField));
                }

                add(
                    proto3MessageOrBuilder.load(),
                    Codegen.invokeProtoMethod(
                        proto3MessageOrBuilder.clazz(),
                        codeGenMapWriter.getter(),
                        field.getFieldDescriptor()),
                    MethodVariableAccess.loadThis());

                add(new StackManipulation.Simple(new StackManipulation.Simple.Dispatcher() {
                  @Override
                  public StackManipulation.Size apply(
                      MethodVisitor methodVisitor, Context implementationContext) {
                    methodVisitor.visitInvokeDynamicInsn(
                        "accept",
                        "("
                            + classBuilder
                                .toTypeDescription()
                                .getDescriptor() + ")Ljava/util/function/BiConsumer;",
                        new Handle(
                            Opcodes.H_INVOKESTATIC,
                            "java/lang/invoke/LambdaMetafactory",
                            "metafactory",
                            "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)Ljava/lang/invoke/CallSite;",
                            false),
                        JavaConstantValue.Visitor.INSTANCE.onMethodType(
                            JavaConstant.MethodType.of(
                                void.class, Object.class, Object.class)),
                        new Handle(
                            Opcodes.H_INVOKEVIRTUAL,
                            classBuilder
                                .toTypeDescription()
                                .getInternalName(),
                            methodDescription.getInternalName(),
                            methodDescription.getDescriptor(),
                            false),
                        JavaConstantValue.Visitor.INSTANCE.onMethodType(
                            JavaConstant.MethodType.of(
                                TypeDescription.ForLoadedType.of(void.class),
                                keyType,
                                valueType)));
                    return StackManipulation.Size.ZERO;
                  }
                }));
                add(Codegen.invokeMethod(
                    ReflectionUtil.getDeclaredMethod(Map.class, "forEach", BiConsumer.class)));

                if (protoWriteSupport.isWriteSpecsCompliant()) {
                  add(
                      recordConsumerVar.load(),
                      new TextConstant("key_value"),
                      IntegerConstant.forValue(0),
                      Codegen.invokeMethod(Reflection.RecordConsumer.endField),
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.endGroup));
                }

                add(
                    recordConsumerVar.load(),
                    new TextConstant(field.getParquetFieldName()),
                    IntegerConstant.forValue(field.getParquetFieldIndex()),
                    Codegen.invokeMethod(Reflection.RecordConsumer.endField));

                add(Codegen.visitLabel(after), localVars.frameEmptyStack());
              }
            };
          }

          protected Implementation writeNonMessageRegularField(Field field, LocalVar proto3MessageOrBuilder) {
            if (field.isPrimitive()) {
              return writePrimitiveField(proto3MessageOrBuilder, recordConsumerVar, field);
            } else if (field.isBinary()) {
              return writeBinaryField(proto3MessageOrBuilder, recordConsumerVar, field);
            } else if (field.isString()) {
              return writeStringField(proto3MessageOrBuilder, recordConsumerVar, field);
            } else if (field.isProtoWrapper()) {
              return writeProtoWrapperField(proto3MessageOrBuilder, recordConsumerVar, field);
            } else if (field.isEnum()) {
              return writeEnumField(proto3MessageOrBuilder, recordConsumerVar, field);
            }
            throw new CodeGenException("field: " + field);
          }
        }

        class BinaryFieldWriter extends RegularFieldWriterTemplate {
          BinaryFieldWriter(Field field, LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }
          /*
          ByteString byteString = (ByteString) value;
          Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
          recordConsumer.addBinary(binary);
          */

          @Override
          void convertRawValueAndWrite() {
            add(
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(ByteString.class, "toByteArray")),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                    Binary.class, "fromConstantByteArray", byte[].class)),
                Codegen.invokeMethod(Reflection.RecordConsumer.addBinary));
          }
        }

        class StringFieldWriter extends RegularFieldWriterTemplate {
          StringFieldWriter(Field field, LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          /*
          Binary binaryString = Binary.fromString((String) value);
          recordConsumer.addBinary(binaryString);
          */
          @Override
          void convertRawValueAndWrite() {
            add(
                Codegen.invokeMethod(
                    ReflectionUtil.getDeclaredMethod(Binary.class, "fromString", String.class)),
                Codegen.invokeMethod(Reflection.RecordConsumer.addBinary));
          }
        }

        class ProtoWrapperFieldWriter extends RegularFieldWriterTemplate {
          ProtoWrapperFieldWriter(Field field, LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          @Override
          void convertRawValueAndWrite() {
            ProtoWriteSupport<?>.FieldWriter fieldWriter = field.fieldWriter;
            if (fieldWriter instanceof ProtoWriteSupport<?>.BytesValueWriter) {
              add(
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(BytesValue.class, "getValue")),
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(ByteString.class, "toByteArray")),
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                      Binary.class, "fromConstantByteArray", byte[].class)),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addBinary));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.StringValueWriter) {
              add(
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(StringValue.class, "getValue")),
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(Binary.class, "fromString", String.class)),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addBinary));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.BoolValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(BoolValue.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addBoolean));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.UInt32ValueWriter) {
              add(
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(UInt32Value.class, "getValue")),
                  Codegen.castIntToLong(),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addLong));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.Int32ValueWriter) {
              add(
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(Int32Value.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addInteger));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.UInt64ValueWriter) {
              add(
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(UInt64Value.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addLong));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.Int64ValueWriter) {
              add(
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(Int64Value.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addLong));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.FloatValueWriter) {
              add(
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(FloatValue.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addFloat));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.DoubleValueWriter) {
              add(
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(DoubleValue.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addDouble));
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.TimeWriter) {
              try (LocalVar timeOfDay = localVars.register(TimeOfDay.class)) {
                add(
                    timeOfDay.store(),
                    timeOfDay.load(),
                    Codegen.invokeMethod(
                        ReflectionUtil.getDeclaredMethod(TimeOfDay.class, "getHours")),
                    timeOfDay.load(),
                    Codegen.invokeMethod(
                        ReflectionUtil.getDeclaredMethod(TimeOfDay.class, "getMinutes")),
                    timeOfDay.load(),
                    Codegen.invokeMethod(
                        ReflectionUtil.getDeclaredMethod(TimeOfDay.class, "getSeconds")),
                    timeOfDay.load(),
                    Codegen.invokeMethod(
                        ReflectionUtil.getDeclaredMethod(TimeOfDay.class, "getNanos")),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                        LocalTime.class, "of", int.class, int.class, int.class, int.class)),
                    Codegen.invokeMethod(
                        ReflectionUtil.getDeclaredMethod(LocalTime.class, "toNanoOfDay")),
                    Codegen.invokeMethod(Reflection.RecordConsumer.addLong));
              }
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.DateWriter) {
              try (LocalVar date = localVars.register(Date.class)) {
                add(
                    date.store(),
                    date.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Date.class, "getYear")),
                    date.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Date.class, "getMonth")),
                    date.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Date.class, "getDay")),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                        LocalDate.class, "of", int.class, int.class, int.class)),
                    Codegen.invokeMethod(
                        ReflectionUtil.getDeclaredMethod(LocalDate.class, "toEpochDay")),
                    Codegen.castLongToInt(),
                    Codegen.invokeMethod(Reflection.RecordConsumer.addInteger));
              }
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.TimestampWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                      Timestamps.class, "toNanos", Timestamp.class)),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addLong));
            } else {
              throw new IllegalStateException();
            }
          }
        }

        class EnumFieldWriter extends RegularFieldWriterTemplate {

          EnumFieldWriter(Field field, LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          String getterMethodTemplate() {
            return "get{}" + (supportsUnknownValues() ? "Value" : "");
          }

          boolean supportsUnknownValues() {
            EnumReflectionType enumReflectionType = (EnumReflectionType) field.getReflectionType();
            return enumReflectionType.enumSupportsUnknownValues
                && enumReflectionType.fieldSupportsUnknownValues;
          }

          @Override
          void loadRepeatedValueOnStack(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            add(
                proto3MessageOrBuilder.load(),
                iVar.load(),
                Codegen.invokeProtoMethod(
                    proto3MessageOrBuilder.clazz(),
                    getterMethodTemplate(),
                    field.getFieldDescriptor(),
                    int.class));
          }

          @Override
          void beforeLoadValueOnStack() {}

          @Override
          void convertRawValueAndWrite() {
            if (supportsUnknownValues()) {
              convertRawValueAndWriteWithUnknownValues();
            } else {
              convertRawValueAndWriteWithoutUnknownValues();
            }
          }

          /**
           *         int enumNumber = messageOrBuilder.getEnumValue();
           *         ProtocolMessageEnum enum_ = forNumber.apply(enumNumber);
           *         Enum<?> javaEnum = (Enum<?>) enum_;
           *         Descriptors.EnumValueDescriptor enumValueDescriptor;
           *         if (javaEnum != null) {
           *           enumValueDescriptor = enumValues.get(javaEnum.ordinal());
           *         } else {
           *           enumValueDescriptor = enumDescriptor.findValueByNumberCreatingIfUnknown(enumNumber);
           *         }
           */
          void convertRawValueAndWriteWithUnknownValues() {
            CodeGenEnum codeGenEnum = enumFields.get(field.getCodeGenerationElementKey());
            Class<?> enumClass = codeGenEnum.clazz;

            try (LocalVar enumNumber = localVars.register(int.class)) {
              add(
                  enumNumber.store(),
                  enumNumber.load(),
                  Codegen.invokeMethod(
                      ReflectionUtil.getDeclaredMethod(enumClass, "forNumber", int.class)));
              try (LocalVar enumRef = localVars.register(enumClass)) {
                add(enumRef.store(), enumRef.load());
                Label ifEnumRefIsNull = new Label();
                Label afterEnumValueResolved = new Label();
                add(
                    Codegen.jumpTo(Opcodes.IFNULL, ifEnumRefIsNull),
                    MethodVariableAccess.loadThis(),
                    FieldAccess.forField(codeGenEnum.enumValues())
                        .read(),
                    enumRef.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Enum.class, "ordinal")),
                    ArrayAccess.REFERENCE.load(),
                    Codegen.jumpTo(Opcodes.GOTO, afterEnumValueResolved),
                    Codegen.visitLabel(ifEnumRefIsNull),
                    localVars.frameEmptyStack(),
                    MethodVariableAccess.loadThis(),
                    FieldAccess.forField(codeGenEnum.enumDescriptor())
                        .read(),
                    enumNumber.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                        Descriptors.EnumDescriptor.class,
                        "findValueByNumberCreatingIfUnknown",
                        int.class)),
                    Codegen.visitLabel(afterEnumValueResolved),
                    localVars.frameSame1(Descriptors.EnumValueDescriptor.class));

                writeEnumValueDesc(codeGenEnum);
              }
            }
          }

          /**
           *         Enum<?> javaEnum = messageOrBuilder.getEnum();
           *         enumValueDescriptor = enumValues.get(javaEnum.ordinal());
           */
          void convertRawValueAndWriteWithoutUnknownValues() {
            CodeGenEnum codeGenEnum = enumFields.get(field.getCodeGenerationElementKey());
            Class<?> enumClass = codeGenEnum.clazz;

            try (LocalVar enumRef = localVars.register(enumClass)) {
              add(
                  enumRef.store(),
                  MethodVariableAccess.loadThis(),
                  FieldAccess.forField(codeGenEnum.enumValues())
                      .read(),
                  enumRef.load(),
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Enum.class, "ordinal")),
                  ArrayAccess.REFERENCE.load());

              writeEnumValueDesc(codeGenEnum);
            }
          }

          private void writeEnumValueDesc(CodeGenEnum codeGenEnum) {
            try (LocalVar enumValueDesc = localVars.register(Descriptors.EnumValueDescriptor.class)) {
              add(enumValueDesc.store());
              try (LocalVar enumValueDescName = localVars.register(String.class)) {
                add(
                    enumValueDesc.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                        Descriptors.EnumValueDescriptor.class, "getName")),
                    enumValueDescName.store(),
                    enumValueDescName.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                        Binary.class, "fromString", String.class)));
                try (LocalVar binary = localVars.register(Binary.class)) {
                  add(
                      binary.store(),
                      recordConsumerVar.load(),
                      binary.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.addBinary),
                      MethodVariableAccess.loadThis(),
                      FieldAccess.forField(codeGenEnum.enumNumberPairs())
                          .read(),
                      enumValueDescName.load(),
                      enumValueDesc.load(),
                      Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                          Descriptors.EnumValueDescriptor.class, "getNumber")),
                      Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                          Integer.class, "valueOf", int.class)),
                      Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(
                          Map.class, "putIfAbsent", Object.class, Object.class)),
                      Removal.SINGLE);
                }
              }
            }
          }
        }
      }

      class WriteAllFieldsForMessageImplementation extends FastMessageWriterMethodBase {
        WriteAllFieldsForMessageImplementation(Field field) {

          try (LocalVar thisLocalVar = localVars.register(classBuilder.toTypeDescription())) {
            writeMessageFields(field);
          }
        }

        private void writeMessageFields(Field field) {
          Class<? extends MessageOrBuilder> messageOrBuilderInterface = field.getMessageOrBuilderInterface();

          try (LocalVar messageOrBuilderArg = localVars.register(messageOrBuilderInterface)) {
            localVars.frameEmptyStack();

            try (LocalVar proto3MessageOrBuilder = messageOrBuilderArg.alias()) {
              try (LocalVar recordConsumerVar = localVars.register(RecordConsumer.class)) {
                add(Codegen.storeRecordConsumer(recordConsumerVar));
                add(new MessageFieldWriter(field, recordConsumerVar)
                    .writeMessageFieldsInternal(proto3MessageOrBuilder));
              }
            }
            add(Codegen.returnVoid());
          }
        }
      }

      class WriteAllFieldsForMapEntryImplementation extends FastMessageWriterMethodBase {

        WriteAllFieldsForMapEntryImplementation(Field field) {
          CodeGenMapWriter codeGenMapWriter = mapWriters.get(field.getCodeGenerationElementKey());
          Class<?>[] methodParameters = codeGenMapWriter.writeMapEntryParameters();
          try (LocalVar thisLocalVar = localVars.register(classBuilder.toTypeDescription())) {
            try (LocalVar key = localVars.register(methodParameters[0])) {
              try (LocalVar value = localVars.register(methodParameters[1])) {
                try (LocalVar recordConsumerVar = localVars.register(RecordConsumer.class)) {
                  add(Codegen.storeRecordConsumer(recordConsumerVar));
                  add(
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.startGroup));
                  add(writeFromVar(field.mapKey(), key, recordConsumerVar));
                  add(writeFromVar(field.mapValue(), value, recordConsumerVar));
                  add(
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.endGroup));
                  add(Codegen.returnVoid());
                }
              }
            }
          }
        }

        Implementation writeFromVar(Field field, LocalVar val, LocalVar recordConsumer) {
          if (field.isEnum()) {
            return new EnumFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isProtoMessage()) {
            return new MessageFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isString()) {
            return new StringFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isBinary()) {
            return new BinaryFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isProtoWrapper()) {
            return new ProtoWrapperFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isPrimitive()) {
            return new PrimitiveFieldWriter(field, recordConsumer).writeFromVar(val);
          }
          throw new CodeGenException("field: " + field);
        }
      }

      static class CodeGenElement {
        private final int id;
        private final Field field;

        public CodeGenElement(int id, Field field) {
          this.id = id;
          this.field = field;
        }

        public Field getField() {
          return field;
        }

        public int getId() {
          return id;
        }
      }

      class CodeGenMessageWriter extends CodeGenElement {
        private final Class<? extends MessageOrBuilder> messageOrBuilderInterface;

        public CodeGenMessageWriter(int id, Field field) {
          super(id, field);
          this.messageOrBuilderInterface = field.getMessageOrBuilderInterface();
        }

        public String getMethodName() {
          return "writeAllFields$" + getId();
        }

        public Class<? extends MessageOrBuilder> getMethodParameterType() {
          return messageOrBuilderInterface;
        }

        public MethodDescription getMethodDescription() {
          return new MethodDescription.Latent(
              classBuilder.toTypeDescription(),
              new MethodDescription.Token(
                  getMethodName(),
                  Visibility.PUBLIC.getMask(),
                  TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(void.class),
                  Collections.singletonList(TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(
                      getMethodParameterType()))));
        }
      }

      class CodeGenEnum extends CodeGenElement {
        private final String enumTypeFullName;
        private final Class<?> clazz;

        public CodeGenEnum(int id, Field field) {
          super(id, field);
          enumTypeFullName = field.getFieldDescriptor().getEnumType().getFullName();
          clazz = ((EnumReflectionType) field.getReflectionType()).clazz;
        }

        public Class<?> getEnumClass() {
          return clazz;
        }

        public String getEnumTypeFullName() {
          return enumTypeFullName;
        }

        // final Map<String, Integer> enumNameNumberPairs<idx>;
        public FieldDescription enumNumberPairs() {
          return new FieldDescription.Latent(
              classBuilder.toTypeDescription(),
              new FieldDescription.Token(
                  "enumNameNumberPairs$" + getId(),
                  Modifier.PRIVATE | Modifier.FINAL,
                  TypeDescription.Generic.Builder.parameterizedType(
                          Map.class, String.class, Integer.class)
                      .build()));
        }

        // final Descriptors.EnumDescriptor enumDescriptor<idx>
        public FieldDescription enumDescriptor() {
          return new FieldDescription.Latent(
              classBuilder.toTypeDescription(),
              new FieldDescription.Token(
                  "enumDescriptor$" + getId(),
                  Modifier.PRIVATE | Modifier.FINAL,
                  new TypeDescription.Generic.OfNonGenericType.ForLoadedType(
                      Descriptors.EnumDescriptor.class)));
        }

        // final List<Descriptors.EnumValueDescriptor> enumValues<idx>
        public FieldDescription enumValues() {
          return new FieldDescription.Latent(
              classBuilder.toTypeDescription(),
              new FieldDescription.Token(
                  "enumValues$" + getId(),
                  Modifier.PRIVATE | Modifier.FINAL,
                  TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(
                      Descriptors.EnumValueDescriptor[].class)));
        }
      }

      class CodeGenMapWriter extends CodeGenElement {
        public CodeGenMapWriter(int id, Field field) {
          super(id, field);
        }

        public String getMethodName() {
          return "writeMapEntry$" + getId();
        }

        public MethodDescription writeMapEntry() {
          Class<?>[] parameters = writeMapEntryParameters();
          return new MethodDescription.Latent(
              classBuilder.toTypeDescription(),
              new MethodDescription.Token(
                  getMethodName(),
                  Visibility.PUBLIC.getMask(),
                  TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(void.class),
                  Arrays.asList(
                      TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(parameters[0]),
                      TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(parameters[1]))));
        }

        public Class<?>[] writeMapEntryParameters() {
          MapReflectionType mapReflectionType =
              (MapReflectionType) getField().getReflectionType();
          Class<?> keyType = (Class<?>) mapReflectionType.key();
          Class<?> valueType = getValueType(mapReflectionType);
          return new Class[] {keyType, valueType};
        }

        public String getter() {
          MapReflectionType mapReflectionType =
              (MapReflectionType) getField().getReflectionType();
          boolean isEnumAndSupportsUnknownValues = false;
          if (mapReflectionType.value() instanceof EnumReflectionType) {
            EnumReflectionType enumReflectionType = (EnumReflectionType) mapReflectionType.value();
            isEnumAndSupportsUnknownValues = enumReflectionType.enumSupportsUnknownValues
                && enumReflectionType.fieldSupportsUnknownValues;
          }
          return "get{}" + (isEnumAndSupportsUnknownValues ? "Value" : "") + "Map";
        }

        private Class<?> getValueType(MapReflectionType mapReflectionType) {
          Class<?> valueType;
          if (mapReflectionType.value() instanceof EnumReflectionType) {
            EnumReflectionType enumReflectionType = (EnumReflectionType) mapReflectionType.value();
            if (enumReflectionType.enumSupportsUnknownValues
                && enumReflectionType.fieldSupportsUnknownValues) {
              valueType = int.class;
            } else {
              valueType = enumReflectionType.clazz;
            }
          } else {
            valueType = (Class<?>) mapReflectionType.value();
          }
          return valueType;
        }
      }

      class CodeGenFieldWriterFallback extends CodeGenElement {

        public CodeGenFieldWriterFallback(int id, Field field) {
          super(id, field);
        }

        public FieldDescription fieldWriter() {
          return new FieldDescription.Latent(
              classBuilder.toTypeDescription(),
              new FieldDescription.Token(
                  getFieldName(),
                  Modifier.PRIVATE,
                  // TODO: create more specific FieldWriter
                  TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(
                      ProtoWriteSupport.FieldWriter.class)));
        }

        public String getFieldName() {
          return "fieldWriter$" + getId();
        }
      }

      static class GeneratedElementsInfo {
        private final FieldScanner fieldScanner;
        private final Class<? extends Message> protoMessage;
        private final Descriptors.Descriptor messageType;
        private final Map<Object, Method> codeGenMessageWriters;
        private final Map<Object, Integer> fallbackFieldWriters;

        public GeneratedElementsInfo(
            FieldScanner fieldScanner,
            Class<? extends Message> protoMessage,
            Descriptors.Descriptor messageType,
            Map<Object, Method> codeGenMessageWriters,
            Map<Object, Integer> fallbackFieldWriters) {
          this.fieldScanner = fieldScanner;
          this.messageType = messageType;
          this.protoMessage = protoMessage;
          this.codeGenMessageWriters = codeGenMessageWriters;
          this.fallbackFieldWriters = fallbackFieldWriters;
        }

        public void scan(ProtoWriteSupport<?>.MessageWriter messageWriter, FieldVisitor fieldVisitor) {
          fieldScanner.scan(messageWriter, protoMessage, messageType, fieldVisitor);
        }
      }

      private GeneratedElementsInfo getGeneratedElementsInfo(
          Class<? extends ByteBuddyMessageWriters> generatedClass) {
        Map<Object, Method> codeGenMessageWriters = new LinkedHashMap<>();
        Map<Object, Integer> protoReflectionMessageWriters = new LinkedHashMap<>();

        for (Map.Entry<Object, CodeGenMessageWriter> key2CodeGenMessageWriterEntry :
            this.codeGenMessageWriters.entrySet()) {
          codeGenMessageWriters.put(
              key2CodeGenMessageWriterEntry.getKey(),
              ReflectionUtil.getDeclaredMethod(
                  generatedClass,
                  key2CodeGenMessageWriterEntry.getValue().getMethodName(),
                  key2CodeGenMessageWriterEntry.getValue().getMethodParameterType()));
        }

        for (Map.Entry<Object, CodeGenFieldWriterFallback> key2CodeGenProtoReflectionMessageWriterEntry :
            this.fieldWriterFallbacks.entrySet()) {
          protoReflectionMessageWriters.put(
              key2CodeGenProtoReflectionMessageWriterEntry.getKey(),
              key2CodeGenProtoReflectionMessageWriterEntry
                  .getValue()
                  .getId());
        }

        return new GeneratedElementsInfo(
            fieldScanner, protoMessage, descriptor, codeGenMessageWriters, protoReflectionMessageWriters);
      }

      private static <V> void addCodeGenElement(
          Field field, Map<Object, V> registry, BiFunction<Integer, Field, V> codeElementConstructor) {
        registry.computeIfAbsent(
            field.getCodeGenerationElementKey(),
            unused -> codeElementConstructor.apply(registry.size(), field));
      }

      public Consumer<ProtoWriteSupport<?>.MessageWriter> getPatcher() {
        if (byteBuddyMessageWritersClass == null) {
          return NOOP_WRITER_PATCHER;
        }
        return new ByteBuddyMessageWritersPatcher(
            ReflectionUtil.getConstructor(
                byteBuddyMessageWritersClass,
                ProtoWriteSupport.MessageWriter.class,
                GeneratedElementsInfo.class),
            getGeneratedElementsInfo(byteBuddyMessageWritersClass));
      }
    }

    static class ByteBuddyMessageWritersPatcher implements Consumer<ProtoWriteSupport<?>.MessageWriter> {
      private final Constructor<? extends ByteBuddyMessageWriters> byteBuddyMessageWritersConstructor;
      private final ByteBuddyMessageWritersCodeGen.GeneratedElementsInfo generatedElementsInfo;

      ByteBuddyMessageWritersPatcher(
          Constructor<? extends ByteBuddyMessageWriters> byteBuddyMessageWritersConstructor,
          ByteBuddyMessageWritersCodeGen.GeneratedElementsInfo generatedElementsInfo) {
        this.byteBuddyMessageWritersConstructor = byteBuddyMessageWritersConstructor;
        this.generatedElementsInfo = generatedElementsInfo;
      }

      @Override
      public void accept(ProtoWriteSupport<?>.MessageWriter messageWriter) {
        ReflectionUtil.newInstance(byteBuddyMessageWritersConstructor, messageWriter, generatedElementsInfo);
      }
    }

    // this is subclassed with ByteBuddy, overriding the constructor, setProtoReflectionMessageWriters and adding
    // new fields and methods
    public abstract static class ByteBuddyMessageWriters {
      private final ProtoWriteSupport<?> protoWriteSupport;
      private final Map<Method, ProtoWriteSupport.MessageFieldsWriter> fastMessageWriters = new LinkedHashMap<>();
      private final MethodHandles.Lookup lookup;

      public ByteBuddyMessageWriters(
          ProtoWriteSupport<?>.MessageWriter rootMessageWriter,
          ByteBuddyMessageWritersCodeGen.GeneratedElementsInfo generatedElementsInfo) {
        this.protoWriteSupport = rootMessageWriter.getProtoWriteSupport();
        this.lookup = getLookup();
        final ProtoWriteSupport<?>.FieldWriter[] fallbackFieldWriters =
            new ProtoWriteSupport<?>.FieldWriter[generatedElementsInfo.fallbackFieldWriters.size()];

        // assign alternative message writers and collect protobuf reflection message writers
        generatedElementsInfo.scan(rootMessageWriter, new FieldVisitor() {
          @Override
          public void visitField(Field field) {
            if (field.isFieldWriterFallback()) {
              if (field.isFieldWriterFallbackTransition()) {
                Object key = field.getCodeGenerationElementKey();
                int id = generatedElementsInfo.fallbackFieldWriters.get(key);
                if (fallbackFieldWriters[id] == null) {
                  fallbackFieldWriters[id] = field.fieldWriter;
                }
              }
            } else if (field.isMessage()) {
              Object key = field.getCodeGenerationElementKey();
              Method method = generatedElementsInfo.codeGenMessageWriters.get(key);
              field.getMessageWriter().setAlternativeMessageWriter(getFastMessageWriter(method));
            }
          }
        });

        for (ProtoWriteSupport<?>.FieldWriter fieldWriter : fallbackFieldWriters) {
          if (fieldWriter == null) {
            throw new CodeGenException();
          }
        }
        setFallbackFieldWriters(fallbackFieldWriters);
      }

      // the implementation needs to assign the passed array to fields
      public void setFallbackFieldWriters(ProtoWriteSupport<?>.FieldWriter[] fieldWriters) {}

      // used from the generated methods to load record consumer in a local variable
      public RecordConsumer getRecordConsumer() {
        return protoWriteSupport.getRecordConsumer();
      }

      // used from the constructor, when assigning the maps for enums
      public Map<String, Integer> enumNameNumberPairs(String enumTypeFullName) {
        return protoWriteSupport.getProtoEnumBookKeeper().get(enumTypeFullName);
      }

      public ProtoWriteSupport.MessageFieldsWriter getFastMessageWriter(Method method) {
        return fastMessageWriters.computeIfAbsent(method, k -> {
          Class<?> messageOrBuilderInterface = method.getParameterTypes()[0];
          try {
            Consumer<MessageOrBuilder> consumer =
                (Consumer<MessageOrBuilder>) LambdaMetafactory.metafactory(
                        lookup,
                        "accept",
                        MethodType.methodType(Consumer.class, this.getClass()),
                        MethodType.methodType(void.class, Object.class),
                        lookup.unreflect(method),
                        MethodType.methodType(void.class, messageOrBuilderInterface))
                    .getTarget()
                    .bindTo(this)
                    .invokeExact();
            return new ProtoWriteSupport.MessageFieldsWriter() {
              @Override
              public boolean writeAllFields(MessageOrBuilder messageOrBuilder) {
                if (!messageOrBuilderInterface.isInstance(messageOrBuilder)) {
                  return false;
                }
                consumer.accept(messageOrBuilder);
                return true;
              }
            };
          } catch (Throwable e) {
            throw new CodeGenException(e);
          }
        });
      }

      protected abstract MethodHandles.Lookup getLookup();
    }
  }

  public static class ReadSupport {

    static <T extends Message> RecordMaterializer<T> tryEnhanceRecordMaterializer(
        org.apache.parquet.proto.ProtoRecordMaterializer<T> protoRecordMaterializer,
        ProtoReadSupport.CodegenMode codegenMode,
        ParquetConfiguration configuration) {

      protoRecordMaterializer = new ProtoRecordMaterializerTransformer(codegenMode, configuration)
          .transform(protoRecordMaterializer);

      return protoRecordMaterializer;
    }

    private static class ProtoRecordMaterializerTransformer<T extends Message> {

      interface MapEntryBuilder {
        void clear();
      }

      private ProtoReadSupport.CodegenMode codegenMode;
      private ParquetConfiguration configuration;

      public ProtoRecordMaterializerTransformer(
          ProtoReadSupport.CodegenMode codegenMode, ParquetConfiguration configuration) {
        this.codegenMode = codegenMode;
        this.configuration = configuration;
      }

      public ProtoRecordMaterializer<T> transform(ProtoRecordMaterializer<T> protoRecordMaterializer) {
        GroupConverter rootConverter = protoRecordMaterializer.getRootConverter();
        if (rootConverter instanceof ProtoMessageConverter) {
          ProtoMessageConverter protoMessageConverter = (ProtoMessageConverter) rootConverter;
          transformRootConverter(protoMessageConverter);
        }
        return protoRecordMaterializer;
      }

      private void transformRootConverter(ProtoMessageConverter messageConverter) {
        Converter[] converters = messageConverter.converters;
        for (int i = 0; i < converters.length; i++) {
          Converter converter = converters[i];
          converters[i] = transformChildConverter(messageConverter.myBuilder, converter);
        }
      }

      private Converter transformChildConverter(Object parentBuilder, Converter converter) {
        if (converter instanceof ProtoMessageConverter) {
          return transformChildConverterProtoMessageConverter(
              parentBuilder, (ProtoMessageConverter) converter);
        }
        if (converter instanceof ProtoEnumConverter) {
          return transformChildConverterProtoEnumConverter(parentBuilder, (ProtoEnumConverter) converter);
        }
        if (converter instanceof ProtoBinaryConverter) {
          return transformChildConverterProtoBinaryConverter(parentBuilder, (ProtoBinaryConverter) converter);
        }
        if (converter instanceof ProtoBooleanConverter) {
          return transformChildConverterProtoBooleanConverter(
              parentBuilder, (ProtoBooleanConverter) converter);
        }
        if (converter instanceof ProtoDoubleConverter) {
          return transformChildConverterProtoDoubleConverter(parentBuilder, (ProtoDoubleConverter) converter);
        }
        if (converter instanceof ProtoFloatConverter) {
          return transformChildConverterProtoFloatConverter(parentBuilder, (ProtoFloatConverter) converter);
        }
        if (converter instanceof ProtoIntConverter) {
          return transformChildConverterProtoIntConverter(parentBuilder, (ProtoIntConverter) converter);
        }
        if (converter instanceof ProtoLongConverter) {
          return transformChildConverterProtoLongConverter(parentBuilder, (ProtoLongConverter) converter);
        }
        if (converter instanceof ProtoStringConverter) {
          return transformChildConverterProtoStringConverter(parentBuilder, (ProtoStringConverter) converter);
        }
        if (converter instanceof ProtoTimestampConverter) {
          return transformChildConverterProtoTimestampConverter(
              parentBuilder, (ProtoTimestampConverter) converter);
        }
        if (converter instanceof ProtoDateConverter) {
          return transformChildConverterProtoDateConverter(parentBuilder, (ProtoDateConverter) converter);
        }
        if (converter instanceof ProtoTimeConverter) {
          return transformChildConverterProtoTimeConverter(parentBuilder, (ProtoTimeConverter) converter);
        }
        if (converter instanceof ProtoDoubleValueConverter) {
          return transformChildConverterProtoDoubleValueConverter(
              parentBuilder, (ProtoDoubleValueConverter) converter);
        }
        if (converter instanceof ProtoFloatValueConverter) {
          return transformChildConverterProtoFloatValueConverter(
              parentBuilder, (ProtoFloatValueConverter) converter);
        }
        if (converter instanceof ProtoInt64ValueConverter) {
          return transformChildConverterProtoInt64ValueConverter(
              parentBuilder, (ProtoInt64ValueConverter) converter);
        }
        if (converter instanceof ProtoUInt64ValueConverter) {
          return transformChildConverterProtoUInt64ValueConverter(
              parentBuilder, (ProtoUInt64ValueConverter) converter);
        }
        if (converter instanceof ProtoInt32ValueConverter) {
          return transformChildConverterProtoInt32ValueConverter(
              parentBuilder, (ProtoInt32ValueConverter) converter);
        }
        if (converter instanceof ProtoUInt32ValueConverter) {
          return transformChildConverterProtoUInt32ValueConverter(
              parentBuilder, (ProtoUInt32ValueConverter) converter);
        }
        if (converter instanceof ProtoBoolValueConverter) {
          return transformChildConverterProtoBoolValueConverter(
              parentBuilder, (ProtoBoolValueConverter) converter);
        }
        if (converter instanceof ProtoStringValueConverter) {
          return transformChildConverterProtoStringValueConverter(
              parentBuilder, (ProtoStringValueConverter) converter);
        }
        if (converter instanceof ProtoBytesValueConverter) {
          return transformChildConverterProtoBytesValueConverter(
              parentBuilder, (ProtoBytesValueConverter) converter);
        }
        if (converter instanceof MapConverter) {
          return transformChildConverterMapConverter(parentBuilder, (MapConverter) converter);
        }
        if (converter instanceof ListConverter) {
          return transformChildConverterListConverter(parentBuilder, (ListConverter) converter);
        }
        return converter;
      }

      private Converter transformChildConverterProtoEnumConverter(
          Object parentBuilder, ProtoEnumConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoEnumConverter(generatePvc(
            parentBuilder,
            fieldDescriptor,
            Descriptors.EnumValueDescriptor.class
        ), converter);
      }

      private Converter transformChildConverterListConverter(Object parentBuilder, ListConverter converter) {
        Converter listConverter = transformChildConverter(parentBuilder, converter.converter.converter);

        GroupConverter wrapperConverter = new GroupConverter() {
          @Override
          public Converter getConverter(int fieldIndex) {
            return listConverter;
          }

          @Override
          public void start() {}

          @Override
          public void end() {}
        };

        return new GroupConverter() {
          @Override
          public Converter getConverter(int fieldIndex) {
            return wrapperConverter;
          }

          @Override
          public void start() {}

          @Override
          public void end() {}
        };
      }

      private Converter transformChildConverterMapConverter(Object parentBuilder, MapConverter converter) {
        Converter mapConverter = transformChildConverter(parentBuilder, converter.converter);

        return new GroupConverter() {
          @Override
          public Converter getConverter(int fieldIndex) {
            return mapConverter;
          }

          @Override
          public void start() {}

          @Override
          public void end() {}
        };
      }

      private Converter transformChildConverterProtoBytesValueConverter(
          Object parentBuilder, ProtoBytesValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoBytesValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                BytesValue.class
            )
        );
      }

      private Converter transformChildConverterProtoStringValueConverter(
          Object parentBuilder, ProtoStringValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoStringValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                StringValue.class
            )
        );
      }

      private Converter transformChildConverterProtoBoolValueConverter(
          Object parentBuilder, ProtoBoolValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoBoolValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                BoolValue.class
            )
        );
      }

      private Converter transformChildConverterProtoUInt32ValueConverter(
          Object parentBuilder, ProtoUInt32ValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoUInt32ValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                UInt32Value.class
            )
        );
      }

      private Converter transformChildConverterProtoInt32ValueConverter(
          Object parentBuilder, ProtoInt32ValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoInt32ValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                Int32Value.class
            )
        );
      }

      private Converter transformChildConverterProtoUInt64ValueConverter(
          Object parentBuilder, ProtoUInt64ValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoUInt64ValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                UInt64Value.class
            )
        );
      }

      private Converter transformChildConverterProtoInt64ValueConverter(
          Object parentBuilder, ProtoInt64ValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoInt64ValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                Int64Value.class
            )
        );
      }

      private Converter transformChildConverterProtoFloatValueConverter(
          Object parentBuilder, ProtoFloatValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoFloatValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                FloatValue.class
            )
        );
      }

      private Converter transformChildConverterProtoDoubleValueConverter(
          Object parentBuilder, ProtoDoubleValueConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoDoubleValueConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                DoubleValue.class
            )
        );
      }

      private Converter transformChildConverterProtoTimeConverter(
          Object parentBuilder, ProtoTimeConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoTimeConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                TimeOfDay.class
            ), converter.logicalTypeAnnotation
        );
      }

      private Converter transformChildConverterProtoDateConverter(
          Object parentBuilder, ProtoDateConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoDateConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                Date.class
            )
        );
      }

      private Converter transformChildConverterProtoTimestampConverter(
          Object parentBuilder, ProtoTimestampConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoTimestampConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                Timestamp.class
            ), converter.logicalTypeAnnotation
        );
      }

      private Converter transformChildConverterProtoBinaryConverter(
          Object parentBuilder, ProtoBinaryConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoBinaryConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                ByteString.class
            )
        );
      }

      private Converter transformChildConverterProtoStringConverter(
          Object parentBuilder, ProtoStringConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoStringConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                String.class
            )
        );
      }

      private Converter transformChildConverterProtoLongConverter(
          Object parentBuilder, ProtoLongConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoLongConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                long.class
            )
        );
      }

      private Converter transformChildConverterProtoIntConverter(
          Object parentBuilder, ProtoIntConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoIntConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                int.class
            )
        );
      }

      private Converter transformChildConverterProtoFloatConverter(
          Object parentBuilder, ProtoFloatConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoFloatConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                float.class
            )
        );
      }

      private Converter transformChildConverterProtoDoubleConverter(
          Object parentBuilder, ProtoDoubleConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoDoubleConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                double.class
            )
        );
      }

      private Converter transformChildConverterProtoBooleanConverter(
          Object parentBuilder, ProtoBooleanConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        return new ProtoBooleanConverter(
            generatePvc(
                parentBuilder,
                fieldDescriptor,
                boolean.class
            )
        );
      }

      private Converter transformChildConverterProtoMessageConverter(
          Object parentBuilder, ProtoMessageConverter converter) {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(converter.parent);

        if (fieldDescriptor == null) {
          return converter;
        }

        Object myBuilder = fieldDescriptor.isMapField()
            ? newMapEntryBuilder(parentBuilder, fieldDescriptor)
            : converter.myBuilder;

        Converter[] converters = converter.converters;
        Converter[] newConverters = new Converter[converters.length];
        for (int i = 0; i < converters.length; i++) {
          Converter childConverter = converters[i];
          newConverters[i] = transformChildConverter(myBuilder, childConverter);
        }

        ParentValueContainer newPvc = generatePvc(parentBuilder, fieldDescriptor, myBuilder.getClass());

        return new PreBuiltProtoMessageConverter(newConverters, newPvc, myBuilder);
      }

      private Object newMapEntryBuilder(Object parentBuilder, Descriptors.FieldDescriptor fieldDescriptor) {
        return new Supplier<Object>() {
          private DynamicType.Builder<Object> classBuilder;

          @Override
          public Object get() {
            List<Descriptors.FieldDescriptor> mapFields = fieldDescriptor.getMessageType().getFields();
            Descriptors.FieldDescriptor keyField = mapFields.get(0);
            Descriptors.FieldDescriptor valueField = mapFields.get(1);
            Class<?> keyType = getMapEntryKeyType(parentBuilder.getClass(), keyField);
            Class<?> valueType = getMapEntryValueType(parentBuilder.getClass(), fieldDescriptor, valueField);
            String setValueMethodName = valueField.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM
                && int.class.equals(valueType)
                ? "setValueValue" : "setValue";

            classBuilder = new ByteBuddy()
                .subclass(Object.class)
                .modifiers(Visibility.PUBLIC)
                .name(ByteBuddyCodeGen.class.getName() + "$MapBuilder$Generated$"
                    + BYTE_BUDDY_CLASS_SEQUENCE.incrementAndGet());

            MethodDescription.Latent clearMethodDesc = new MethodDescription.Latent(
                classBuilder.toTypeDescription(),
                new MethodDescription.Token(
                    "clear",
                    Visibility.PUBLIC.getMask(),
                    TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(void.class),
                    Collections.emptyList()));

            TypeDescription.Generic keyTypeGen = TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(keyType);
            TypeDescription.Generic valueTypeGen = TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(valueType);

            FieldDescription.Latent keyFieldDesc = new FieldDescription.Latent(
                classBuilder.toTypeDescription(),
                new FieldDescription.Token(
                    "key",
                    Visibility.PRIVATE.getMask(),
                    keyTypeGen,
                    Collections.emptyList()));

            FieldDescription.Latent valueFieldDesc = new FieldDescription.Latent(
                classBuilder.toTypeDescription(),
                new FieldDescription.Token(
                    "value",
                    Visibility.PRIVATE.getMask(),
                    valueTypeGen,
                    Collections.emptyList()));

            MethodDescription.Latent getKeyMethodDesc = new MethodDescription.Latent(
                classBuilder.toTypeDescription(),
                new MethodDescription.Token(
                    "getKey",
                    Visibility.PUBLIC.getMask(),
                    keyTypeGen,
                    Collections.emptyList()));

            MethodDescription.Latent getValueMethodDesc = new MethodDescription.Latent(
                classBuilder.toTypeDescription(),
                new MethodDescription.Token(
                    "getValue",
                    Visibility.PUBLIC.getMask(),
                    valueTypeGen,
                    Collections.emptyList()));

            MethodDescription.Latent setKeyMethodDesc = new MethodDescription.Latent(
                classBuilder.toTypeDescription(),
                new MethodDescription.Token(
                    "setKey",
                    Visibility.PUBLIC.getMask(),
                    TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(void.class),
                    Collections.singletonList(keyTypeGen)));

            Class<?> valueBuilderType;
            if (valueField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
              try {
                valueBuilderType = valueType.getDeclaredMethod("newBuilder").invoke(null).getClass();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            } else {
              valueBuilderType = null;
            }

            MethodDescription.Latent setValueMethodDesc = new MethodDescription.Latent(
                classBuilder.toTypeDescription(),
                new MethodDescription.Token(
                    setValueMethodName,
                    Visibility.PUBLIC.getMask(),
                    TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(void.class),
                    Collections.singletonList(TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(valueBuilderType != null ? valueBuilderType : valueType))));


            classBuilder = classBuilder
                .constructor(ElementMatchers.any())
                .intercept(MethodCall.invoke(ReflectionUtil.getConstructor(
                        Object.class))
                    .andThen(new Implementations() {
                      {
                        CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                        try (LocalVar thisLocalVar =
                                 localVars.register(classBuilder.toTypeDescription())) {
                          add(
                              MethodVariableAccess.loadThis(),
                              MethodInvocation.invoke(clearMethodDesc));
                        }
                        add(Codegen.returnVoid());
                      }
                    }));

            classBuilder = classBuilder.define(keyFieldDesc);
            classBuilder = classBuilder.define(valueFieldDesc);
            classBuilder = classBuilder.define(getKeyMethodDesc)
                .intercept(new Implementations() {
                  {
                    CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                    try (LocalVar thisLocalVar =
                             localVars.register(classBuilder.toTypeDescription())) {
                      add(
                          MethodVariableAccess.loadThis(),
                          FieldAccess.forField(keyFieldDesc).read());
                    }
                    add(MethodReturn.of(keyTypeGen));
                  }
                });
            classBuilder = classBuilder.define(setKeyMethodDesc)
                .intercept(new Implementations() {
                  {
                    CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                    try (LocalVar thisLocalVar =
                             localVars.register(classBuilder.toTypeDescription())) {
                      try (LocalVar v = localVars.register(TypeDescription.ForLoadedType.of(keyType))) {
                        add(
                            MethodVariableAccess.loadThis(),
                            v.load(),
                            FieldAccess.forField(keyFieldDesc).write());
                      }
                    }
                    add(Codegen.returnVoid());
                  }
                });
            classBuilder = classBuilder.define(getValueMethodDesc)
                .intercept(new Implementations() {
                  {
                    CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                    try (LocalVar thisLocalVar =
                             localVars.register(classBuilder.toTypeDescription())) {
                      add(
                          MethodVariableAccess.loadThis(),
                          FieldAccess.forField(valueFieldDesc).read());
                    }
                    add(MethodReturn.of(valueTypeGen));
                  }
                });
            classBuilder = classBuilder.define(setValueMethodDesc)
                .intercept(new Implementations() {
                  {
                    CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                    try (LocalVar thisLocalVar =
                             localVars.register(classBuilder.toTypeDescription())) {
                      try (LocalVar v = localVars.register(TypeDescription.ForLoadedType.of(valueBuilderType != null ? valueBuilderType : valueType))) {
                        add(
                            MethodVariableAccess.loadThis(),
                            v.load());
                        if (valueBuilderType != null) {
                          add(Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(valueBuilderType, "build")));
                        }
                        add(FieldAccess.forField(valueFieldDesc).write());
                      }
                    }
                    add(Codegen.returnVoid());
                  }
                });
            classBuilder = classBuilder.define(clearMethodDesc)
                .intercept(new Implementations() {
                  {
                    CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                    try (LocalVar thisLocalVar =
                             localVars.register(classBuilder.toTypeDescription())) {
                      add(MethodVariableAccess.loadThis());
                      switch (keyField.getJavaType()) {
                        case INT:
                          add(IntegerConstant.forValue(0));
                          break;
                        case LONG:
                          add(LongConstant.forValue(0L));
                          break;
                        case FLOAT:
                          add(FloatConstant.forValue(0.0f));
                          break;
                        case DOUBLE:
                          add(DoubleConstant.forValue(0.0));
                          break;
                        case BOOLEAN:
                          add(IntegerConstant.forValue(false));
                          break;
                        case STRING:
                          add(new TextConstant(""));
                          break;
                        default:
                          throw new IllegalStateException();
                      }
                      add(FieldAccess.forField(keyFieldDesc).write());
                      add(MethodVariableAccess.loadThis());
                      switch (valueField.getJavaType()) {
                        case INT:
                          add(IntegerConstant.forValue(0));
                          break;
                        case LONG:
                          add(LongConstant.forValue(0L));
                          break;
                        case FLOAT:
                          add(FloatConstant.forValue(0.0f));
                          break;
                        case DOUBLE:
                          add(DoubleConstant.forValue(0.0));
                          break;
                        case BOOLEAN:
                          add(IntegerConstant.forValue(false));
                          break;
                        case STRING:
                          add(new TextConstant(""));
                          break;
                        case ENUM:
                          if (valueType.equals(int.class)) {
                            add(IntegerConstant.forValue(0));
                          } else {
                            add(IntegerConstant.forValue(0),
                                Codegen.invokeMethod(
                                    ReflectionUtil.getDeclaredMethod(valueType, "forNumber", int.class)));
                          }
                          break;
                        case MESSAGE:
                            add(Codegen.invokeMethod(
                                ReflectionUtil.getDeclaredMethod(valueType, "getDefaultInstance")));
                          break;
                        default:
                          throw new IllegalStateException();
                      }
                      add(FieldAccess.forField(valueFieldDesc).write());
                    }
                    add(Codegen.returnVoid());
                  }
                });

            DynamicType.Unloaded<Object> unloaded = classBuilder.make();
            Class<?> mapBuilderClass = unloaded.load(
                    parentBuilder.getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();
            return ReflectionUtil.newInstance(ReflectionUtil.getConstructor(mapBuilderClass));
          }
        }.get();
      }

      private Class<?> getMapEntryValueType(Class<?> messageBuilderClass, Descriptors.FieldDescriptor mapFieldDescriptor, Descriptors.FieldDescriptor valueField) {
        switch (valueField.getJavaType()) {
          case INT:
            return int.class;
          case LONG:
            return long.class;
          case FLOAT:
            return float.class;
          case DOUBLE:
            return double.class;
          case BOOLEAN:
            return boolean.class;
          case STRING:
            return String.class;
          case BYTE_STRING:
            return ByteString.class;
          case ENUM: {
            Descriptors.EnumDescriptor enumType = valueField.getEnumType();
            boolean hasValueSetter = !enumType.isClosed() && !valueField.legacyEnumFieldTreatedAsClosed();
            if (hasValueSetter) {
              return int.class;
            }
          }
        }
        switch (valueField.getJavaType()) {
          case ENUM:
          case MESSAGE: {
            String mapField = ReflectionUtil.getFieldNameForMethod(mapFieldDescriptor);
            List<Method> putMethods = Arrays.stream(messageBuilderClass.getDeclaredMethods()).filter(x -> x.getName().equals("put" + mapField))
                .collect(Collectors.toList());
            if (putMethods.size() != 1) {
              throw new IllegalStateException("Expected one put method for map field: " + mapField);
            }
            Method putMethod = putMethods.get(0);
            Class<?>[] parameterTypes = putMethod.getParameterTypes();
            if (parameterTypes.length != 2) {
              throw new IllegalStateException("Expected two parameters for put method: " + putMethod);
            }
            return parameterTypes[1];
          }
        }
        throw new IllegalStateException("Unsupported value type: " + valueField.getJavaType());
      };

      private Class<?> getMapEntryKeyType(Class<?> messageBuilderClass, Descriptors.FieldDescriptor keyField) {
        switch (keyField.getJavaType()) {
          case INT:
            return int.class;
          case LONG:
            return long.class;
          case FLOAT:
            return float.class;
          case DOUBLE:
            return double.class;
          case BOOLEAN:
            return boolean.class;
          case STRING:
            return String.class;
          default:
            throw new IllegalStateException("Unsupported key type: " + keyField.getJavaType());
        }
      }

      private ParentValueContainer generatePvc(
          Object parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Class<?> valueType) {
        if (!fieldDescriptor.isMapField() && !(parentBuilder instanceof MapEntry.Builder)) {
          return getRegularFieldPvc(parentBuilder, fieldDescriptor, valueType);
        }
        return getMapFieldPvc(parentBuilder, fieldDescriptor, valueType);
//         return getDefaultPvc((Message.Builder) parentBuilder, fieldDescriptor, valueType);
      }

      private ParentValueContainer getMapFieldPvc(Object parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Class<?> mapBuilderType) {
        return new Supplier<ParentValueContainer>() {
          private DynamicType.Builder<ParentValueContainer> classBuilder;

          @Override
          public ParentValueContainer get() {
            Class<?> parentBuilderClass = parentBuilder.getClass();
            String fieldNameForMethod = ReflectionUtil.getFieldNameForMethod(fieldDescriptor);
            TypeDescription parentBuilderTypeDef = TypeDescription.ForLoadedType.of(parentBuilderClass);
            MethodList<MethodDescription.InDefinedShape> parentBuilderMethods = parentBuilderTypeDef.getDeclaredMethods();
            String setterPrefix = "put";
            String setterSuffix =
                Arrays.stream(mapBuilderType.getDeclaredMethods()).anyMatch(x -> x.getName().equals("setValueValue"))
                ? "Value" : "";

            ElementMatcher<MethodDescription> setterArgumentMatcher =
                    ElementMatchers.takesArguments(
                        Arrays.stream(mapBuilderType.getDeclaredMethods()).filter(x -> x.getName().equals("getKey"))
                            .map(x -> x.getReturnType()).findFirst().get(),
                        Arrays.stream(mapBuilderType.getDeclaredMethods()).filter(x -> x.getName().equals("getValue"))
                            .map(x -> x.getReturnType()).findFirst().get()
                    );

            MethodDescription.InDefinedShape parentBuilderSetter =
                parentBuilderMethods.filter(
                    ElementMatchers.named(setterPrefix + fieldNameForMethod + setterSuffix).and(
                        setterArgumentMatcher
                    )
                ).getOnly();

            classBuilder = new ByteBuddy()
                .subclass(ParentValueContainer.class)
                .modifiers(Visibility.PUBLIC)
                .name(ParentValueContainer.class.getName() + "$Generated$"
                    + BYTE_BUDDY_CLASS_SEQUENCE.incrementAndGet());

            TypeDescription.Generic parentBuilderType =
                TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(parentBuilderClass);
            FieldDescription.Latent parentBuilderFieldDesc = new FieldDescription.Latent(
                classBuilder.toTypeDescription(),
                new FieldDescription.Token(
                    "parent", Modifier.PRIVATE | Modifier.FINAL, parentBuilderType));
            classBuilder = classBuilder.define(parentBuilderFieldDesc);

            classBuilder = classBuilder
                .define(new MethodDescription.Latent(
                    classBuilder.toTypeDescription(),
                    new MethodDescription.Token(
                        MethodDescription.CONSTRUCTOR_INTERNAL_NAME,
                        Visibility.PUBLIC.getMask(),
                        TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(void.class),
                        Collections.singletonList(parentBuilderType))))
                .intercept(MethodCall.invoke(ReflectionUtil.getConstructor(
                        ParentValueContainer.class))
                    .andThen(new Implementations() {
                      {
                        CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                        try (LocalVar thisLocalVar =
                                 localVars.register(classBuilder.toTypeDescription())) {
                          try (LocalVar parentVar = localVars.register(parentBuilderClass)) {
                            add(
                                MethodVariableAccess.loadThis(),
                                parentVar.load(),
                                FieldAccess.forField(parentBuilderFieldDesc)
                                    .write());
                          }
                        }
                        add(Codegen.returnVoid());
                      }
                    }));

            String pvcMethodNameSuffix = "";

            classBuilder = classBuilder
                .method(ElementMatchers.named("add" + pvcMethodNameSuffix))
                .intercept(new Implementations() {
                  {
                    CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                    try (LocalVar thisLocalVar =
                             localVars.register(classBuilder.toTypeDescription())) {
                      try (LocalVar valueVar = localVars.register(Object.class)) {
                        add(
                            MethodVariableAccess.loadThis(),
                            FieldAccess.forField(parentBuilderFieldDesc)
                                .read(),
                            valueVar.load(),
                            TypeCasting.to(TypeDescription.ForLoadedType.of(mapBuilderType)),
                            Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(mapBuilderType, "getKey")),
                            valueVar.load(),
                            TypeCasting.to(TypeDescription.ForLoadedType.of(mapBuilderType)),
                            Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(mapBuilderType, "getValue")),
                            MethodInvocation.invoke(parentBuilderSetter),
                            valueVar.load(),
                            TypeCasting.to(TypeDescription.ForLoadedType.of(mapBuilderType)),
                            Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(mapBuilderType, "clear")));
                        add(Codegen.returnVoid());
                      }
                    }
                  }
                });

            DynamicType.Unloaded<ParentValueContainer> unloaded = classBuilder.make();
            Class<? extends ParentValueContainer> pvcClass = unloaded.load(
                    mapBuilderType.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();
            return ReflectionUtil.newInstance(
                ReflectionUtil.getConstructor(pvcClass, parentBuilderClass), parentBuilder);
          }
        }.get();
      }

      private ParentValueContainer getRegularFieldPvc(Object parentBuilder,
                                                      Descriptors.FieldDescriptor fieldDescriptor,
                                                      Class<?> valueType) {
        return new Supplier<ParentValueContainer>() {
          private DynamicType.Builder<ParentValueContainer> classBuilder;

          @Override
          public ParentValueContainer get() {
            Class<?> parentBuilderClass = parentBuilder.getClass();

            String fieldNameForMethod = ReflectionUtil.getFieldNameForMethod(fieldDescriptor);
            TypeDescription parentBuilderTypeDef = TypeDescription.ForLoadedType.of(parentBuilderClass);
            MethodList<MethodDescription.InDefinedShape> parentBuilderMethods = parentBuilderTypeDef.getDeclaredMethods();
            String setterPrefix = fieldDescriptor.isRepeated() ? "add" : "set";
            boolean isEnum = fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.ENUM;
            boolean supportUnknownValues = isEnum
                && !fieldDescriptor.getEnumType().isClosed()
                && !fieldDescriptor.legacyEnumFieldTreatedAsClosed();
            String setterSuffix = supportUnknownValues ? "Value" : "";
            TypeDescription enumType = isEnum ?
              parentBuilderMethods
                  .filter(ElementMatchers.named(setterPrefix + fieldNameForMethod + setterSuffix))
                  .getOnly()
                  .asTypeToken()
                  .getParameterTypes()
                  .get(0)
                : null;

            boolean isMessageBuilder = Message.Builder.class.isAssignableFrom(valueType);
            boolean isMessage = Message.class.isAssignableFrom(valueType);
            ElementMatcher<MethodDescription> setterArgumentMatcher =
                isMessageBuilder || isMessage
                    ? ElementMatchers.takesArguments(valueType)
                    : ElementMatchers.any();

            MethodDescription.InDefinedShape parentBuilderSetter =
                parentBuilderMethods.filter(
                ElementMatchers.named(setterPrefix + fieldNameForMethod + setterSuffix).and(
                    setterArgumentMatcher
                )
            ).getOnly();

            classBuilder = new ByteBuddy()
                .subclass(ParentValueContainer.class)
                .modifiers(Visibility.PUBLIC)
                .name(ParentValueContainer.class.getName() + "$Generated$"
                    + BYTE_BUDDY_CLASS_SEQUENCE.incrementAndGet());

            TypeDescription.Generic parentBuilderType =
                TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(parentBuilderClass);
            FieldDescription.Latent parentBuilderFieldDesc = new FieldDescription.Latent(
                classBuilder.toTypeDescription(),
                new FieldDescription.Token(
                    "parent", Modifier.PRIVATE | Modifier.FINAL, parentBuilderType));
            classBuilder = classBuilder.define(parentBuilderFieldDesc);

            classBuilder = classBuilder
                .define(new MethodDescription.Latent(
                    classBuilder.toTypeDescription(),
                    new MethodDescription.Token(
                        MethodDescription.CONSTRUCTOR_INTERNAL_NAME,
                        Visibility.PUBLIC.getMask(),
                        TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(void.class),
                        Collections.singletonList(parentBuilderType))))
                .intercept(MethodCall.invoke(ReflectionUtil.getConstructor(
                        ParentValueContainer.class))
                    .andThen(new Implementations() {
                      {
                        CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                        try (LocalVar thisLocalVar =
                                 localVars.register(classBuilder.toTypeDescription())) {
                          try (LocalVar parentVar = localVars.register(parentBuilderClass)) {
                            add(
                                MethodVariableAccess.loadThis(),
                                parentVar.load(),
                                FieldAccess.forField(parentBuilderFieldDesc)
                                    .write());
                          }
                        }
                        add(Codegen.returnVoid());
                      }
                    }));

            String pvcMethodNameSuffix = valueType.isPrimitive() ?
                valueType.getName().substring(0, 1).toUpperCase() + valueType.getName().substring(1) :
                "";

            classBuilder = classBuilder
                .method(ElementMatchers.named("add" + pvcMethodNameSuffix))
                .intercept(new Implementations() {
                  {
                    CodeGenUtils.LocalVars localVars = new CodeGenUtils.LocalVars();
                    try (LocalVar thisLocalVar =
                             localVars.register(classBuilder.toTypeDescription())) {
                      try (LocalVar valueVar = localVars.register(valueType.isPrimitive() ? valueType : Object.class)) {
                        add(
                            MethodVariableAccess.loadThis(),
                            FieldAccess.forField(parentBuilderFieldDesc)
                                .read(),
                            valueVar.load());
                        if (isEnum) {
                          add(TypeCasting.to(
                              TypeDescription.ForLoadedType.of(valueType)));
                          if (supportUnknownValues) {
                            add(Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(valueType, "getNumber")));
                          } else {
                            MethodDescription.InDefinedShape valueOfMethod = enumType.getDeclaredMethods()
                                .filter(ElementMatchers
                                    .hasMethodName("valueOf")
                                    .and(ElementMatchers.takesArguments(Descriptors.EnumValueDescriptor.class)))
                                .getOnly();
                            add(MethodInvocation.invoke(valueOfMethod));
                          }
                        } else if (!valueType.isPrimitive()) {
                          add(TypeCasting.to(
                              TypeDescription.ForLoadedType.of(valueType)));
                        }
                        add(MethodInvocation.invoke(parentBuilderSetter));
                        if (isMessageBuilder) {
                          add(valueVar.load(),
                              TypeCasting.to(
                                  TypeDescription.ForLoadedType.of(valueType)),
                              Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(valueType, "clear"))
                          );
                        }
                        add(Codegen.returnVoid());
                      }
                    }
                  }
                });

            DynamicType.Unloaded<ParentValueContainer> unloaded = classBuilder.make();
            Class<? extends ParentValueContainer> pvcClass = unloaded.load(
                    parentBuilderClass.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();
            return ReflectionUtil.newInstance(
                ReflectionUtil.getConstructor(pvcClass, parentBuilderClass), parentBuilder);
          }
        }.get();
      }

      private static ParentValueContainer getDefaultPvc(Message.Builder parentBuilder,
                                                                  Descriptors.FieldDescriptor fieldDescriptor,
                                                                  Class<?> valueType) {
        ParentValueContainer fallbackPvc = fieldDescriptor.isRepeated()
            ? new AddRepeatedFieldParentValueContainer(parentBuilder, fieldDescriptor)
            : new SetFieldParentValueContainer(parentBuilder, fieldDescriptor);

        boolean isBuilder = Message.Builder.class.isAssignableFrom(valueType);

        return new ParentValueContainer() {
          @Override
          public void add(Object val) {
            if (isBuilder) {
              Message.Builder builder = (Message.Builder) val;
              Message message = builder.build();
              fallbackPvc.add(message);
              builder.clear();
            } else {
              fallbackPvc.add(val);
            }
          }
        };
      }

      private Descriptors.FieldDescriptor getFieldDescriptor(ParentValueContainer pvc) {
        if (pvc instanceof SetFieldParentValueContainer) {
          SetFieldParentValueContainer setFieldPvc = (SetFieldParentValueContainer) pvc;
          return setFieldPvc.getFieldDescriptor();
        }
        if (pvc instanceof AddRepeatedFieldParentValueContainer) {
          AddRepeatedFieldParentValueContainer addRepeatedFieldPvc =
              (AddRepeatedFieldParentValueContainer) pvc;
          return addRepeatedFieldPvc.getFieldDescriptor();
        }
        return null;
      }

      private static class PreBuiltProtoMessageConverter extends GroupConverter {
        protected final Converter[] converters;
        protected final ParentValueContainer parent;
        protected final Object myBuilder;

        public PreBuiltProtoMessageConverter(
            Converter[] converters, ParentValueContainer parent, Object myBuilder) {
          this.converters = converters;
          this.parent = parent;
          this.myBuilder = myBuilder;
        }

        @Override
        public Converter getConverter(int fieldIndex) {
          return converters[fieldIndex];
        }

        @Override
        public void start() {}

        @Override
        public void end() {
          parent.add(myBuilder);
        }
      }
    }
  }
}
