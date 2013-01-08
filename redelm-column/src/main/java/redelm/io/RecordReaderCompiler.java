package redelm.io;

import static brennus.ClassBuilder.startClass;
import static brennus.model.ExistingType.INT;
import static brennus.model.ExistingType.VOID;
import static brennus.model.ExistingType.OBJECT;
import static brennus.model.ExistingType.existing;
import static brennus.model.Protection.PUBLIC;
import static brennus.model.Protection.PRIVATE;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redelm.column.ColumnReader;
import redelm.column.ColumnsStore;
import redelm.io.RecordReaderImplementation.Case;
import redelm.io.RecordReaderImplementation.State;

import brennus.CaseBuilder;
import brennus.ClassBuilder;
import brennus.ConstructorBuilder;
import brennus.ElseBuilder;
import brennus.Function;
import brennus.MethodBuilder;
import brennus.MethodDeclarationBuilder;
import brennus.StatementBuilder;
import brennus.SwitchBuilder;
import brennus.ThenBuilder;
import brennus.asm.ASMTypeGenerator;
import brennus.model.ExistingType;
import brennus.model.FutureType;
import brennus.printer.TypePrinter;

public class RecordReaderCompiler {

  public static abstract class BaseRecordReader<T> extends RecordReader<T> {
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

    protected int getCaseId(int state, int currentLevel, int d, int nextR) {
      return caseLookup[state].getCase(currentLevel, d, nextR).getID();
    }

    protected void startMessage() {
      recordMaterializer.startMessage();
    }

    protected void startGroup(int state, int depth) {
      String field = caseLookup[state].fieldPath[depth];
      int index = caseLookup[state].indexFieldPath[depth];
      recordMaterializer.startField(field, index);
      recordMaterializer.startGroup();
    }

    protected void addPrimitiveINT64(int state, int depth, long value) {
      String field = caseLookup[state].fieldPath[depth];
      int index = caseLookup[state].indexFieldPath[depth];
      recordMaterializer.startField(field, index);
      recordMaterializer.addLong(value);
      recordMaterializer.endField(field, index);
    }

    protected void addPrimitiveSTRING(int state, int depth, String value) {
      String field = caseLookup[state].fieldPath[depth];
      int index = caseLookup[state].indexFieldPath[depth];
      recordMaterializer.startField(field, index);
      recordMaterializer.addString(value);
      recordMaterializer.endField(field, index);
    }

    protected void addPrimitiveINT32(int state, int depth, int value) {
      String field = caseLookup[state].fieldPath[depth];
      int index = caseLookup[state].indexFieldPath[depth];
      recordMaterializer.startField(field, index);
      recordMaterializer.addInteger(value);
      recordMaterializer.endField(field, index);
    }

    protected void endGroup(int state, int depth) {
      recordMaterializer.endGroup();
      String field = caseLookup[state].fieldPath[depth];
      int index = caseLookup[state].indexFieldPath[depth];
      recordMaterializer.endField(field, index);
    }

    protected void endMessage() {
      recordMaterializer.endMessage();
    }

    protected void error(String message) {
      throw new RuntimeException(message);
    }
  }

  public static class DynamicClassLoader extends ClassLoader {
    ASMTypeGenerator asmTypeGenerator = new ASMTypeGenerator();

    public Class<?> define(FutureType type) {
      new TypePrinter().print(type);
      byte[] classBytes = asmTypeGenerator.generate(type);
      return super.defineClass(type.getName(), classBytes, 0, classBytes.length);
    }
  }

  private DynamicClassLoader cl = new DynamicClassLoader();
  private int id = 0;


  private <S extends StatementBuilder<S>> S generateSwitch(S builder, final boolean addPrimitive, final State state) {
    String columnReader = "state_"+state.id+"_column";
    if (addPrimitive) {
      builder = builder
          .var(existing(state.primitive.javaType), "value_"+state.id)
          .set("value_"+state.id).get(columnReader).callNoParam(state.primitive.getMethod).endSet();
    }
    return builder
        .exec().get(columnReader).callNoParam("consume").endExec()
        .set("nextR").get(columnReader).callNoParam("getCurrentRepetitionLevel").endSet()
        .switchOn()
          .callOnThis("getCaseId").literal(state.id).nextParam().get("currentLevel").nextParam().get("d").nextParam().get("nextR").endCall()
            .switchBlock().transform(
              new Function<SwitchBuilder<S>, SwitchBuilder<S>>() {
                public SwitchBuilder<S> apply(SwitchBuilder<S> builder) {
                  for (Case currentCase : state.getCases()) {
                    CaseBuilder<S> caseBlock = builder
                      .caseBlock(currentCase.getID());
                    if (currentCase.isGoingUp()) {
                      //  for (; currentLevel <= depth; ++currentLevel) {
                      //    startGroup(currentState, currentLevel);
                      //  }
                      for (int i = currentCase.getStartLevel(); i <= currentCase.getDepth(); i++) {
                        // TODO: pass proper value here. Not state id
                        caseBlock = caseBlock.exec().callOnThis("startGroup").literal(state.id).nextParam().literal(i).endCall().endExec();
                      }
                    }
                    if (addPrimitive) {
                      //  addPrimitive(currentState, currentLevel, columnReader);
                      // TODO: finalize method and params
                      caseBlock =
                          caseBlock.exec().callOnThis("addPrimitive"+state.primitive.name())
                            .literal(state.id).nextParam().literal(currentCase.getDepth() + 1).nextParam().get("value_"+state.id)
                          .endCall().endExec();
                    }
                    if (currentCase.isGoingDown()) {
                      //  for (; currentLevel > next; currentLevel--) {
                      //    endGroup(currentState, currentLevel - 1);
                      //  }
                      for (int i = currentCase.getDepth() + 1; i > currentCase.getNextLevel(); i--) {
                        // TODO: pass proper value here. Not state id
                        caseBlock = caseBlock.exec().callOnThis("endGroup").literal(state.id).nextParam().literal(i - 1).endCall().endExec();
                      }
                    }
                    builder = caseBlock.breakCase();
                  }
                  return builder;
                }
              })
          .defaultCase()
            .exec().callOnThis("error").literal("unknown case").endCall().endExec()
          .breakCase()
        .endSwitch();
  }

  public <T> RecordReader<T> compile(final RecordReaderImplementation<T> recordReader) {
    int stateCount = recordReader.getStateCount();
    String className = "redelm.io.RecordReaderCompiler$CompiledRecordReader"+(++id);
    ClassBuilder classBuilder = startClass(className, existing(BaseRecordReader.class));
    for (int i = 0; i < stateCount; i++) {
      classBuilder = classBuilder
        .field(PUBLIC, existing(ColumnReader.class), "state_"+i+"_column")
        .field(PUBLIC, existing(PrimitiveColumnIO.class), "state_"+i+"_primitiveColumnIO");
    }

    MethodBuilder readMethodBuilder = classBuilder
        .startMethod(PUBLIC, VOID, "readOneRecord")
        .var(INT, "currentLevel")
        .var(INT, "d")
        .var(INT, "nextR")
        //  startMessage();
        .exec().callOnThisNoParam("startMessage").endExec()
        .set("currentLevel").literal(0).endSet();

    for (int i = 0; i < stateCount; i++) {
      final State state = recordReader.getState(i);
      String columnReader = "state_"+i+"_column";
      readMethodBuilder = readMethodBuilder
          .label("state_"+i)
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
          .endIf()
          .switchOn().get("nextR").switchBlock()
          .transform(new Function<SwitchBuilder<MethodBuilder>, SwitchBuilder<MethodBuilder>>() {
            public SwitchBuilder<MethodBuilder> apply(SwitchBuilder<MethodBuilder> builder) {
              for (int i = 0; i <= state.maxRepetitionLevel; i++) {
                int nextReader = recordReader.getNextReader(state.id, i);
                String label = nextReader == recordReader.getStateCount() ? "endRecord" : "state_" + nextReader;
                builder = builder.caseBlock(i)
                    .gotoLabel(label)
                    .breakCase();
              }
              return builder;
            }
          })
          .defaultCase()
            .exec().callOnThis("error").literal("unknown transition").endCall().endExec()
          .breakCase()
          .endSwitch();
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
          generated.getField("state_"+i+"_primitiveColumnIO").set(compiledRecordReader, state.primitiveColumnIO);
        } catch (NoSuchFieldException e) {
          throw new RuntimeException("bug: can't find field for state " + i, e);
        }
        compiledRecordReader.caseLookup[i] = state;
      }
      compiledRecordReader.recordMaterializer = recordReader.getMaterializer();
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
}
