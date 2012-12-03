package redelm.io;

import static brennus.ClassBuilder.startClass;
import static brennus.model.ExistingType.VOID;
import static brennus.model.ExistingType.existing;
import static brennus.model.Protection.PUBLIC;
import static brennus.model.Protection.PRIVATE;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;

import redelm.column.ColumnReader;

import brennus.CaseBuilder;
import brennus.ClassBuilder;
import brennus.ConstructorBuilder;
import brennus.MethodBuilder;
import brennus.MethodDeclarationBuilder;
import brennus.SwitchBuilder;
import brennus.asm.ASMTypeGenerator;
import brennus.model.FutureType;

public class RecordReaderCompiler {

  public static class DynamicClassLoader extends ClassLoader {
    ASMTypeGenerator asmTypeGenerator = new ASMTypeGenerator();

    public Class<?> define(FutureType type) {
      byte[] classBytes = asmTypeGenerator.generate(type);
      return super.defineClass(type.getName(), classBytes, 0, classBytes.length);
    }
  }

  private DynamicClassLoader cl = new DynamicClassLoader();
  private int id = 0;

  public RecordReader compile(RecordReader recordReader) {
    int stateCount = recordReader.getStateCount();
    String className = "redelm.io.RecordReaderCompiler$CompiledRecordReader"+(++id);
    ClassBuilder classBuilder = startClass(className, existing(RecordReader.class));
    for (int i = 0; i < stateCount; i++) {
      classBuilder = classBuilder
        .field(PRIVATE, existing(ColumnReader.class), "column_"+i)
        .field(PRIVATE, existing(PrimitiveColumnIO.class), "leaf_"+i);
    }
    ConstructorBuilder constructorBuilder = classBuilder
        .startConstructor(PUBLIC).param(existing(MessageColumnIO.class), "root")
        .callSuperConstructor().get("root").endConstructorCall();

    for (int i = 0; i < stateCount; i++) {
      constructorBuilder = constructorBuilder
        .set("column_"+i).callOnThis("getColumn").literal(i).endCall().endSet()
        .set("leaf_"+i).callOnThis("getLeaf").literal(i).endCall().endSet();
    }
    MethodBuilder readMethodBuilder = constructorBuilder
        .endConstructor()
        .startMethod(PUBLIC, VOID, "read").param(existing(RecordConsumer.class), "recordConsumer")
            .exec().callOnThis("startMessage").get("recordConsumer").endCall().endExec()
            .set("currentLevel").literal(0).endSet();
            for (int i = 0; i < stateCount; i++) {
              // columnReader is "column_"+i;
              // primitiveColumnIO is "leaf_"+i;
              readMethodBuilder = readMethodBuilder
                  .label("state_"+i)
              //  int d = columnReader.getCurrentDefinitionLevel();
                  .set("d").get("column_"+i).callNoParam("getCurrentDefinitionLevel").endSet();
              // creating needed nested groups until the current field (opening tags)
              SwitchBuilder<MethodBuilder> switchBlock = readMethodBuilder
                  .switchOn().get("currentLevel").switchBlock();
//      for (; currentLevel < (primitiveColumnIO.getFieldPath().length - 1)
//          && d > currentNodePath[currentLevel].getDefinitionLevel(); ++currentLevel) {
//        startGroup(recordConsumer, currentNodePath, currentLevel, primitiveColumnIO);
//      }

              //TODO: find the lowest of
              // d > currentNodePath[i].getDefinitionLevel()
              for (int j = 0; j < recordReader.getLeaf(i).getFieldPath().length - 1 ; j++) {
                switchBlock = switchBlock
                    .caseBlock(j)
                      .exec().callOnThis("startGroup")
                        .get("recordConsumer").nextParam()
                        .get("nodePath_"+i).nextParam()
                        .literal(j).nextParam()
                        .get("leaf_"+i).endCall()
                      .endExec()
                      .set("currentLevel").literal(j).endSet()
                      .ifExp().get("d").isGreaterThan().get("node_path_"+i+"_"+j).thenBlock().gotoLabel("endSwitch").endIf()
                    .endCase();
              }
              readMethodBuilder = switchBlock.endSwitch().label("endSwitch");

            }

//      // set the current value
//      if (d >= primitiveColumnIO.getDefinitionLevel()) {
//        // not null
//        String field = primitiveColumnIO.getFieldPath()[currentLevel];
//        int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel];
//        if (DEBUG) log(field+"(" + currentLevel + ") = "+primitiveColumnIO.getType().asPrimitiveType().getPrimitive().toString(columnReader));
//        addPrimitive(recordConsumer, columnReader, primitiveColumnIO.getType().asPrimitiveType().getPrimitive(), field, fieldIndex);
//      }
//      columnReader.consume();
//      int nextR = primitiveColumnIO.getRepetitionLevel() == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
//      int nextCol = nextReader[currentCol][nextR];
//
//      // level to go to close current groups
//      int next = nextLevel[currentCol][nextR];
//      for (; currentLevel > next; currentLevel--) {
//        String field = primitiveColumnIO.getFieldPath()[currentLevel-1];
//        int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel-1];
//        endGroup(recordConsumer, field, fieldIndex);
//      }
//      currentCol = nextCol;
//    } while (currentCol < leaves.length);
    FutureType testClass = readMethodBuilder
            .exec().callOnThis("endMessage").get("recordConsumer").endCall().endExec()
          .endMethod()
        .endClass();

    cl.define(testClass);
    try {
      Class<?> generated = (Class<?>)cl.loadClass(className);
      return (RecordReader)generated.getConstructor(MessageColumnIO.class).newInstance(recordReader.getRoot());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("generated class "+className+" could not be loaded", e);
    } catch (InstantiationException e) {
      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("generated class "+className+" is not accessible", e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
    } catch (SecurityException e) {
      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
    }
  }
}
