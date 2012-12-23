package redelm.io;

import static brennus.ClassBuilder.startClass;
import static brennus.model.ExistingType.INT;
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
import brennus.model.ExistingType;
import brennus.model.FutureType;
import brennus.printer.TypePrinter;

public class RecordReaderCompiler {

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

  public RecordReader compile(RecordReader recordReader) {
    return null;
  }
//    int stateCount = recordReader.getStateCount();
//    String className = "redelm.io.RecordReaderCompiler$CompiledRecordReader"+(++id);
//    ClassBuilder classBuilder = startClass(className, existing(RecordReader.class))
//        // TODO: add support for local variables in Brennus instead
//        .field(PRIVATE, INT, "currentLevel")
//        .field(PRIVATE, INT, "d")
//        .field(PRIVATE, INT, "nextR");
//    for (int i = 0; i < stateCount; i++) {
//      classBuilder = classBuilder
//        .field(PRIVATE, existing(ColumnReader.class), "column_"+i)
//        .field(PRIVATE, existing(PrimitiveColumnIO.class), "leaf_"+i);
//    }
//    ConstructorBuilder constructorBuilder = classBuilder
//        .startConstructor(PUBLIC).param(existing(MessageColumnIO.class), "root")
//        .callSuperConstructor().get("root").endConstructorCall();
//
//    for (int i = 0; i < stateCount; i++) {
//      constructorBuilder = constructorBuilder
//        .set("column_"+i).callOnThis("getColumn").literal(i).endCall().endSet()
//        .set("leaf_"+i).callOnThis("getLeaf").literal(i).endCall().endSet();
//    }
//    MethodBuilder readMethodBuilder = constructorBuilder
//        .endConstructor()
//        .startMethod(PUBLIC, VOID, "read").param(existing(RecordConsumer.class), "recordConsumer")
//            .exec().callOnThis("startMessage").get("recordConsumer").endCall().endExec()
//            .set("currentLevel").literal(0).endSet();
//            for (int i = 0; i < stateCount; i++) {
//              String columnReader = "column_"+i;
//              String primitiveColumnIO = "leaf_"+i;
//              readMethodBuilder = readMethodBuilder
//                  .label("state_"+i)
//              //  int d = columnReader.getCurrentDefinitionLevel();
//                  .set("d").get(columnReader).callNoParam("getCurrentDefinitionLevel").endSet();
//              // creating needed nested groups until the current field (opening tags)
//              if (0 < recordReader.getLeaf(i).getFieldPath().length - 1) {
//                SwitchBuilder<MethodBuilder> switchBlock = readMethodBuilder
//                    .switchOn().get("currentLevel").switchBlock();
//                //            for (; currentLevel < (primitiveColumnIO.getFieldPath().length - 1)
//                //              && d > getDefinitionLevel(currentLevel, primitiveColumnIO); ++currentLevel) {
//                //              startGroup(recordConsumer, currentLevel, primitiveColumnIO);
//                //            }
//                // j takes any possible value of currentLevel
//                for (int j = 0; j < recordReader.getLeaf(i).getFieldPath().length - 1 ; j++) {
//                  switchBlock = switchBlock
//                      .caseBlock(j)
//                        .exec().callOnThis("startGroup")
//                                .get("recordConsumer").nextParam()
//                                .literal(j).nextParam()
//                                .get(primitiveColumnIO).endCall()
//                        .endExec()
//                        .ifExp().get("d").isGreaterThan().callOnThis("getDefinitionLevel").literal(j).nextParam().get(primitiveColumnIO).endCall()
//                        .thenBlock()
//                          .set("currentLevel").literal(j).endSet()
//                          .gotoLabel("endSwitch")
//                        .endIf()
//                      .endCase();
//                }
//                switchBlock = switchBlock.defaultCase().breakCase();
//
//                readMethodBuilder = switchBlock.endSwitch();
//              }
//              readMethodBuilder
//                  .set("currentLevel").literal(recordReader.getLeaf(i).getFieldPath().length - 2).endSet()
//                  // not null
////        String field = primitiveColumnIO.getFieldPath(currentLevel);
////        int fieldIndex = primitiveColumnIO.getIndexFieldPath(currentLevel);
////        if (DEBUG) log(field+"(" + currentLevel + ") = "+primitiveColumnIO.getType().asPrimitiveType().getPrimitive().toString(columnReader));
//                  //      // set the current value
////        addPrimitive(recordConsumer, columnReader, primitiveColumnIO.getPrimitive(), field, fieldIndex);
//                  .exec().callOnThis("addPrimitive")
//                    .get("recordConsumer").nextParam()
//                    .get(columnReader).nextParam()
//                    .get(primitiveColumnIO).callNoParam("getPrimitive").nextParam()
//                    .get(primitiveColumnIO).call("getFieldPath").get("currentLevel").endCall().nextParam()
//                    .get(primitiveColumnIO).call("getIndexFieldPath").get("currentLevel").endCall().endCall()
//                  .endExec()
//                  .label("endSwitch")
////      columnReader.consume();
//                  .exec().get(columnReader).callNoParam("consume").endExec()
////      int nextR = primitiveColumnIO.getRepetitionLevel() == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
//                  .set("nextR").get(columnReader).callNoParam("getCurrentRepetitionLevel").endSet();
////      int nextCol = nextReader[currentCol][nextR];
//              SwitchBuilder<MethodBuilder> nextStateSwitch = readMethodBuilder
//                    .switchOn().get("nextR").switchBlock();
//
//              for (int r = 0; r <= recordReader.getLeaf(i).getRepetitionLevel() ; r++) {
//                CaseBuilder<MethodBuilder> caseBlock = nextStateSwitch.caseBlock(r);
//                if (recordReader.getNextReader(i, r) == stateCount) {
//                        caseBlock = caseBlock
//                            .gotoLabel("endRecord");
//                      } else {
//                        caseBlock = caseBlock
//                            .gotoLabel("state_"+recordReader.getNextReader(i, r));
//                      }
//                nextStateSwitch = caseBlock.endCase();
//
//              }
//              readMethodBuilder = nextStateSwitch.endSwitch();
//
//            }
////
////
////      // level to go to close current groups
////      int next = nextLevel[currentCol][nextR];
////      for (; currentLevel > next; currentLevel--) {
////        String field = primitiveColumnIO.getFieldPath()[currentLevel-1];
////        int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel-1];
////        endGroup(recordConsumer, field, fieldIndex);
////      }
////      currentCol = nextCol;
////    } while (currentCol < leaves.length);
//    FutureType testClass = readMethodBuilder
//            .label("endRecord")
//            .exec().callOnThis("endMessage").get("recordConsumer").endCall().endExec()
//          .endMethod()
//        .endClass();
//
//    cl.define(testClass);
//    try {
//      Class<?> generated = (Class<?>)cl.loadClass(className);
//      return (RecordReader)generated.getConstructor(MessageColumnIO.class).newInstance(recordReader.getRoot());
//    } catch (ClassNotFoundException e) {
//      throw new RuntimeException("generated class "+className+" could not be loaded", e);
//    } catch (InstantiationException e) {
//      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
//    } catch (IllegalAccessException e) {
//      throw new RuntimeException("generated class "+className+" is not accessible", e);
//    } catch (IllegalArgumentException e) {
//      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
//    } catch (SecurityException e) {
//      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
//    } catch (InvocationTargetException e) {
//      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
//    } catch (NoSuchMethodException e) {
//      throw new RuntimeException("generated class "+className+" could not be instanciated", e);
//    }
//  }
}
