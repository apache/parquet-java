package redelm.io;

import static brennus.ClassBuilder.startClass;
import static brennus.model.ExistingType.INT;
import static brennus.model.ExistingType.VOID;
import static brennus.model.ExistingType.existing;
import static brennus.model.Protection.PUBLIC;
import static redelm.Log.DEBUG;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import redelm.Log;
import redelm.column.ColumnReader;
import redelm.column.ColumnsStore;
import redelm.io.RecordReaderImplementation.Case;
import redelm.io.RecordReaderImplementation.State;
import brennus.CaseBuilder;
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

  public static abstract class BaseRecordReader<T> extends RecordReader<T> {
    private static final Log LOG = Log.getLog(BaseRecordReader.class);

    public RecordConsumer recordConsumer;
    public RecordMaterializer<T> recordMaterializer;
    public ColumnsStore columnStore;
    @Override
    public T read() {
      readOneRecord();
      return recordMaterializer.getCurrentRecord();
    }

    /* (non-Javadoc)
     * @see redelm.io.RecordReader#read(T[], int)
     */
    @Override
    public void read(T[] records, int count) {
      if (count > records.length) {
        throw new IllegalArgumentException("count is greater than records size");
      }
      for (int i = 0; i < count; i++) {
        readOneRecord();
        records[i] = recordMaterializer.getCurrentRecord();
      }
    }

    protected abstract void readOneRecord();

    private State[] caseLookup;

    private String endField;

    private int endIndex;

    protected void currentLevel(int currentLevel) {
      if (DEBUG) LOG.debug("currentLevel: "+currentLevel);
    }

    protected void log(String message) {
      if (DEBUG) LOG.debug("bc: "+message);
    }

    final protected int getCaseId(int state, int currentLevel, int d, int nextR) {
      return caseLookup[state].getCase(currentLevel, d, nextR).getID();
    }

    final protected void startMessage() {
      // reset state
      endField = null;
      if (DEBUG) LOG.debug("startMessage()");
      recordConsumer.startMessage();
    }

    final protected void startGroup(String field, int index) {
      startField(field, index);
      if (DEBUG) LOG.debug("startGroup()");
      recordConsumer.startGroup();
    }

    private void startField(String field, int index) {
      if (DEBUG) LOG.debug("startField("+field+","+index+")");
      if (endField != null && index == endIndex) {
        // skip the close/open tag
        endField = null;
      } else {
        if (endField != null) {
          // close the previous field
          recordConsumer.endField(endField, endIndex);
          endField = null;
        }
        recordConsumer.startField(field, index);
      }
    }

    final protected void addPrimitiveINT64(String field, int index, long value) {
      startField(field, index);
      if (DEBUG) LOG.debug("addLong("+value+")");
      recordConsumer.addLong(value);
      endField(field, index);
    }

    private void endField(String field, int index) {
      if (DEBUG) LOG.debug("endField("+field+","+index+")");
      if (endField != null) {
        recordConsumer.endField(endField, endIndex);
      }
      endField = field;
      endIndex = index;
    }

    final protected void addPrimitiveSTRING(String field, int index, String value) {
      startField(field, index);
      if (DEBUG) LOG.debug("addString("+value+")");
      recordConsumer.addString(value);
      endField(field, index);
    }

    final protected void addPrimitiveINT32(String field, int index, int value) {
      startField(field, index);
      if (DEBUG) LOG.debug("addInteger("+value+")");
      recordConsumer.addInteger(value);
      endField(field, index);
    }

    final protected void endGroup(String field, int index) {
      if (endField != null) {
        // close the previous field
        recordConsumer.endField(endField, endIndex);
        endField = null;
      }
      if (DEBUG) LOG.debug("endGroup()");
      recordConsumer.endGroup();
      endField(field, index);
    }

    final protected void endMessage() {
      if (endField != null) {
        // close the previous field
        recordConsumer.endField(endField, endIndex);
        endField = null;
      }
      if (DEBUG) LOG.debug("endMessage()");
      recordConsumer.endMessage();
    }

    protected void error(String message) {
      throw new RuntimeException(message);
    }
  }

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

  private <S extends StatementBuilder<S>> S generateCase(boolean addPrimitive, State state, Case currentCase, S builder) {
    if (currentCase.isGoingUp()) {
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
      //  addPrimitive(currentState, currentLevel, columnReader);
      builder =
          builder.exec().callOnThis("addPrimitive"+state.primitive.name())
            .literal(state.primitiveField).nextParam().literal(state.primitiveFieldIndex).nextParam().get("value_"+state.id)
          .endCall().endExec();
    }
    if (currentCase.isGoingDown()) {
      //  for (; currentLevel > next; currentLevel--) {
      //    endGroup(currentState, currentLevel - 1);
      //  }
      for (int i = currentCase.getDepth() + 1; i > currentCase.getNextLevel(); i--) {
        String field = state.fieldPath[i - 1];
        int index = state.indexFieldPath[i - 1];
        builder = builder.exec().callOnThis("endGroup").literal(field).nextParam().literal(index).endCall().endExec();
      }
    }
    if (currentCase.isGoingDown()) {
      builder = builder
          .set("currentLevel").literal(currentCase.getNextLevel()).endSet();
    } else if (currentCase.isGoingUp()) {
      builder = builder
          .set("currentLevel").literal(currentCase.getDepth() + 1).endSet();
    } else {
      // stays the same
    }
    return builder;
  }

  private <S extends StatementBuilder<S>> S generateSwitch(S builder, final boolean addPrimitive, final State state) {
    String columnReader = "state_"+state.id+"_column";
    if (addPrimitive) {
      builder = builder
          .var(existing(state.primitive.javaType), "value_"+state.id)
          .set("value_"+state.id).get(columnReader).callNoParam(state.primitive.getMethod).endSet();
    }
    builder = builder
        .exec().get(columnReader).callNoParam("consume").endExec();
    if (state.maxRepetitionLevel == 0) {
      builder = builder // TODO: instead change the case lookup code
          .set("nextR").literal(0).endSet();
    } else {
      builder = builder
          .set("nextR").get(columnReader).callNoParam("getCurrentRepetitionLevel").endSet();
    }
    if (state.getCases().size() == 1) {
      final Case currentCase = state.getCases().iterator().next();
      return generateCase(addPrimitive, state, currentCase, builder);
    } else {
      return builder
        .switchOn()
          .callOnThis("getCaseId").literal(state.id).nextParam().get("currentLevel").nextParam().get("d").nextParam().get("nextR").endCall()
            .switchBlock().transform(
              new Function<SwitchBuilder<S>, SwitchBuilder<S>>() {
                public SwitchBuilder<S> apply(SwitchBuilder<S> builder) {
                  for (Case currentCase : state.getCases()) {
                    builder =
                        generateCase(addPrimitive, state, currentCase, builder.caseBlock(currentCase.getID()))
                        .breakCase();
                  }
                  return builder;
                }
              })
          .defaultCase()
            .exec().callOnThis("error").literal("unknown case").endCall().endExec()
          .breakCase()
        .endSwitch();
    }
  }

  public <T> RecordReader<T> compile(final RecordReaderImplementation<T> recordReader) {
    int stateCount = recordReader.getStateCount();
    String className = "redelm.io.RecordReaderCompiler$CompiledRecordReader"+(++id);
    ClassBuilder classBuilder = startClass(className, existing(BaseRecordReader.class));
    for (int i = 0; i < stateCount; i++) {
      classBuilder = classBuilder
        .field(PUBLIC, existing(ColumnReader.class), "state_"+i+"_column");
    }

    MethodBuilder readMethodBuilder = classBuilder
        .startMethod(PUBLIC, VOID, "readOneRecord")
        .var(INT, "currentLevel")
        .var(INT, "d")
        .var(INT, "nextR")
        //  startMessage();
        .transform(this.<MethodBuilder>debug("startMessage"))
        .exec().callOnThisNoParam("startMessage").endExec()
        .set("currentLevel").literal(0).endSet();
    for (int i = 0; i < stateCount; i++) {
      final State state = recordReader.getState(i);
      String columnReader = "state_"+i+"_column";
      readMethodBuilder = readMethodBuilder
          .label("state_"+i)
          .transform(this.<MethodBuilder>debug("state "+i));

      if (state.maxDefinitionLevel == 0) {
        readMethodBuilder = generateSwitch(readMethodBuilder, true, state);
      } else {
        readMethodBuilder = readMethodBuilder
            //  int d = columnReader.getCurrentDefinitionLevel();
            .set("d").get(columnReader).callNoParam("getCurrentDefinitionLevel").endSet()
            .ifExp().get("d").isEqualTo().literal(state.maxDefinitionLevel).thenBlock()
              .transform(new Function<ThenBuilder<MethodBuilder>, ThenBuilder<MethodBuilder>>() {
                public ThenBuilder<MethodBuilder> apply(ThenBuilder<MethodBuilder> builder) {
                  return generateSwitch(builder, true, state);
                }
              })
            .elseBlock()
              .transform(new Function<ElseBuilder<MethodBuilder>, ElseBuilder<MethodBuilder>>() {
                public ElseBuilder<MethodBuilder> apply(ElseBuilder<MethodBuilder> builder) {
                  return generateSwitch(builder, false, state);
                }
              })
            .endIf();
      }
      int firstTransitionNextReader = recordReader.getNextReader(state.id, 0);
      boolean allTheSame = true;
      for (int j = 0; j <= state.maxRepetitionLevel; j++) {
        if (recordReader.getNextReader(state.id, j) != firstTransitionNextReader) {
          allTheSame = false;
          break;
        }
      }
      if (allTheSame) { // if there's only one transition, no need to switch
        int nextReader = firstTransitionNextReader;
        if (nextReader != (state.id + 1)) { // if this is next state, just keep going
          String label = nextReader == recordReader.getStateCount() ? "endRecord" : "state_" + nextReader;
          readMethodBuilder = readMethodBuilder.gotoLabel(label);
        }
      } else {
        readMethodBuilder = readMethodBuilder
            .switchOn().get("nextR").switchBlock()
              .transform(new Function<SwitchBuilder<MethodBuilder>, SwitchBuilder<MethodBuilder>>() {
                public SwitchBuilder<MethodBuilder> apply(SwitchBuilder<MethodBuilder> builder) {
                  for (int i = 0; i <= state.maxRepetitionLevel; i++) {
                    int nextReader = recordReader.getNextReader(state.id, i);
                    String label = nextReader == recordReader.getStateCount() ? "endRecord" : "state_" + nextReader;
                    builder = builder.gotoLabel(i, label);
                  }
                  return builder;
                }
              })
              .defaultCase()
                .exec().callOnThis("error").literal("unknown transition").endCall().endExec()
              .breakCase()
            .endSwitch();
      }
    }

    FutureType testClass = readMethodBuilder
            .label("endRecord")
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
