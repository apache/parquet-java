/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.io;

import static brennus.model.ExistingType.INT;
import static brennus.model.ExistingType.VOID;
import static brennus.model.ExistingType.existing;
import static brennus.model.Protection.PUBLIC;
import static parquet.Log.DEBUG;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import parquet.column.ColumnReader;
import parquet.io.RecordReaderImplementation.Case;
import parquet.io.RecordReaderImplementation.State;

import brennus.Builder;
import brennus.ClassBuilder;
import brennus.ElseBuilder;
import brennus.Function;
import brennus.MethodBuilder;
import brennus.StatementBuilder;
import brennus.SwitchBuilder;
import brennus.ThenBuilder;
import brennus.asm.ASMTypeGenerator;
import brennus.model.FutureType;
import brennus.printer.TypePrinter;

public class RecordReaderCompiler {

  private static final String END_RECORD = "endRecord";

  public static class DynamicClassLoader extends ClassLoader {
    ASMTypeGenerator asmTypeGenerator = new ASMTypeGenerator();

    public Class<?> define(FutureType type) {
      if (DEBUG) new TypePrinter().print(type);
      byte[] classBytes = asmTypeGenerator.generate(type);
      return super.defineClass(type.getName(), classBytes, 0, classBytes.length);
    }
  }

  private DynamicClassLoader cl = new DynamicClassLoader();
  private int id = 0;

  private <S extends StatementBuilder<S>> S generateCase(boolean addPrimitive, State state, Case currentCase, S builder, int stateCount) {
    if (currentCase.isGoingUp()) {
      // Generating the following loop
      //  for (; currentLevel <= depth; ++currentLevel) {
      //    startGroup(currentState, currentLevel);
      //  }
      for (int i = currentCase.getStartLevel(); i <= currentCase.getDepth(); i++) {
        String field = state.fieldPath[i];
        int index = state.indexFieldPath[i];
        builder = builder.exec().callOnThis("startGroup").literal(field).nextParam().literal(index).endCall().endExec();
      }
    }
    if (addPrimitive) {
      // Generating the following call
      //  addPrimitive(currentState, currentLevel, columnReader);
      builder =
          builder.exec().callOnThis("addPrimitive"+state.primitive.name())
            .literal(state.primitiveField).nextParam().literal(state.primitiveFieldIndex).nextParam().get("value_"+state.id)
          .endCall().endExec();
    }
    if (currentCase.isGoingDown()) {
      // Generating the following loop
      //  for (; currentLevel > next; currentLevel--) {
      //    endGroup(currentState, currentLevel - 1);
      //  }
      for (int i = currentCase.getDepth() + 1; i > currentCase.getNextLevel(); i--) {
        String field = state.fieldPath[i - 1];
        int index = state.indexFieldPath[i - 1];
        builder = builder.exec().callOnThis("endGroup").literal(field).nextParam().literal(index).endCall().endExec();
      }
    }
    // set currentLevel to its new value
    if (currentCase.isGoingDown()) {
      builder = builder
          .set("currentLevel").literal(currentCase.getNextLevel()).endSet();
    } else if (currentCase.isGoingUp()) {
      builder = builder
          .set("currentLevel").literal(currentCase.getDepth() + 1).endSet();
    } else {
      // currentLevel stays the same
    }
    int nextReader = currentCase.getNextState();
    String label = getStateLabel(stateCount, nextReader);
    builder = builder.gotoLabel(label);
    return builder;
  }

  private String getStateLabel(int stateCount, int stateId) {
    return stateId == stateCount ? END_RECORD : "state_" + stateId;
  }

  private <S extends StatementBuilder<S>> S generateSwitch(S builder, final boolean defined, final State state, final int stateCount) {
    final List<Case> cases = defined ? state.getDefinedCases() : state.getUndefinedCases();
    String columnReader = "state_"+state.id+"_column";
    if (defined) {
      // if defined we need to save the current value
      builder = builder
          .var(existing(state.primitive.javaType), "value_"+state.id)
          .set("value_"+state.id).get(columnReader).callNoParam(state.primitive.getMethod).endSet();
    }
    builder = builder
        .exec().get(columnReader).callNoParam("consume").endExec();
    if (state.maxRepetitionLevel == 0) {
      // in that case nextR is always 0
      builder = builder // TODO: instead change the case lookup code
          .set("nextR").literal(0).endSet();
    } else {
      builder = builder
          .set("nextR").get(columnReader).callNoParam("getCurrentRepetitionLevel").endSet();
    }
    if (cases.size() == 1) {
      // then no need to switch, directly generate the body of the case
      final Case currentCase = cases.get(0);
      return generateCase(defined, state, currentCase, builder, stateCount);
    } else {
      // more than one case: generate a switch
      return builder
        .switchOn()
          .callOnThis("getCaseId").literal(state.id).nextParam().get("currentLevel").nextParam().get("d").nextParam().get("nextR").endCall()
            .switchBlock().transform(
              new Function<SwitchBuilder<S>, SwitchBuilder<S>>() {
                public SwitchBuilder<S> apply(SwitchBuilder<S> builder) {
                  for (Case currentCase : cases) {
                    if (currentCase.isGoingUp() || defined || currentCase.isGoingDown()) {
                      builder =
                          generateCase(defined, state, currentCase, builder.caseBlock(currentCase.getID()), stateCount)
                          .endCase();
                    } else {
                      // if nothing to do, directly jump to the next state
                      String label = getStateLabel(stateCount, currentCase.getNextState());
                      builder = builder.gotoLabel(currentCase.getID(), label);
                    }
                  }
                  return builder;
                }
              })
          .defaultCase()
            // a default case to be safe: this should never happen
            .exec().callOnThis("error").literal("unknown case").endCall().endExec()
          .breakCase()
        .endSwitch();
    }
  }

  public <T> RecordReader<T> compile(final RecordReaderImplementation<T> recordReader) {
    final int stateCount = recordReader.getStateCount();
    // create a unique class name
    String className = this.getClass().getName()+"$CompiledRecordReader"+(++id);
    ClassBuilder classBuilder = new Builder(false)
        .startClass(className, existing(BaseRecordReader.class));
    for (int i = 0; i < stateCount; i++) {
      // create a field for each column reader
      classBuilder = classBuilder
        .field(PUBLIC, existing(ColumnReader.class), "state_"+i+"_column");
    }

    MethodBuilder readMethodBuilder = classBuilder
        .startMethod(PUBLIC, VOID, "readOneRecord")
          // declare variables
          .var(INT, "currentLevel")
          .var(INT, "d")
          .var(INT, "nextR")
          // debug statement
          .transform(this.<MethodBuilder>debug("startMessage"))
          //  generating: startMessage();
          .exec().callOnThisNoParam("startMessage").endExec()
          // initially: currentLevel = 0;
          .set("currentLevel").literal(0).endSet();
    for (int i = 0; i < stateCount; i++) {
      // generate the code for each state of the FSA
      final State state = recordReader.getState(i);
      String columnReader = "state_"+i+"_column";
      readMethodBuilder = readMethodBuilder
          .label("state_"+i)
          .transform(this.<MethodBuilder>debug("state "+i));

      if (state.maxDefinitionLevel == 0) {
        // then it is always defined, we can skip the if
        readMethodBuilder = generateSwitch(readMethodBuilder, true, state, stateCount);
      } else {
        readMethodBuilder = readMethodBuilder
            // generating:
            //  int d = columnReader.getCurrentDefinitionLevel();
            .set("d").get(columnReader).callNoParam("getCurrentDefinitionLevel").endSet()
            // if it is defined (d == maxDefinitionLevel) then
            .ifExp().get("d").isEqualTo().literal(state.maxDefinitionLevel).thenBlock()
              .transform(new Function<ThenBuilder<MethodBuilder>, ThenBuilder<MethodBuilder>>() {
                public ThenBuilder<MethodBuilder> apply(ThenBuilder<MethodBuilder> builder) {
                  // generate The switch in the defined case (primitive values will be created)
                  return generateSwitch(builder, true, state, stateCount);
                }
              })
            .elseBlock() // otherwise:
              .transform(new Function<ElseBuilder<MethodBuilder>, ElseBuilder<MethodBuilder>>() {
                public ElseBuilder<MethodBuilder> apply(ElseBuilder<MethodBuilder> builder) {
                  // generate The switch in the undefined case (primitive values will not be created)
                  return generateSwitch(builder, false, state, stateCount);
                }
              })
            .endIf();
      }
    }

    FutureType testClass = readMethodBuilder
            .label(END_RECORD)
            //  endMessage();
            .exec().callOnThisNoParam("endMessage").endExec()
          .endMethod()
        .endClass();

    cl.define(testClass);
    try {
      Class<?> generated = (Class<?>)cl.loadClass(className);
      BaseRecordReader<T> compiledRecordReader = (BaseRecordReader<T>)generated.getConstructor().newInstance();
      compiledRecordReader.caseLookup = new State[stateCount];
      for (int i = 0; i < stateCount; i++) {
        State state = recordReader.getState(i);
        try {
          generated.getField("state_"+i+"_column").set(compiledRecordReader, state.column);
        } catch (NoSuchFieldException e) {
          throw new RuntimeException("bug: can't find field for state " + i, e);
        }
        compiledRecordReader.caseLookup[i] = state;
      }
      compiledRecordReader.recordMaterializer = recordReader.getMaterializer();
      compiledRecordReader.recordConsumer = recordReader.getRecordConsumer();
      return compiledRecordReader;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("generated class "+className+" could not be loaded", e);
    } catch (InstantiationException e) {
      throw new RuntimeException("generated class "+className+" could not be instantiated", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("generated class "+className+" is not accessible", e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("generated class "+className+" could not be instantiated", e);
    } catch (SecurityException e) {
      throw new RuntimeException("generated class "+className+" could not be instantiated", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("generated class "+className+" could not be instantiated", e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("generated class "+className+" could not be instantiated", e);
    }
  }

  private <S extends StatementBuilder<S>> Function<S, S> debug(final String message) {
    return new Function<S, S>() {
      @Override
      public S apply(S builder) {
        if (DEBUG) builder = builder.exec().callOnThis("log").literal(message).endCall().endExec();
        return builder;
      }

    };
  }
}
