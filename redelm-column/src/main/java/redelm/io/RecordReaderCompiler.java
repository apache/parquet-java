package redelm.io;

import static brennus.ClassBuilder.startClass;
import static brennus.model.ExistingType.VOID;
import static brennus.model.ExistingType.existing;
import static brennus.model.Protection.PUBLIC;

import java.lang.reflect.InvocationTargetException;

import redelm.column.ColumnReader;

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
    String className = "redelm.io.RecordReaderCompiler$CompiledRecordReader"+(++id);
    FutureType testClass =
        startClass(className, existing(RecordReader.class))
          .startConstructor(PUBLIC).param(existing(MessageColumnIO.class), "root")
            .callSuperConstructor().get("root").endConstructorCall()
          .endConstructor()
          .startMethod(PUBLIC, VOID, "read").param(existing(RecordConsumer.class), "recordConsumer")
            .exec().callOnThis("startMessage").get("recordConsumer").endCall().endExec()
//             do {
//      ColumnReader columnReader = columns[currentCol];
//      PrimitiveColumnIO primitiveColumnIO = leaves[currentCol];
//      int d = columnReader.getCurrentDefinitionLevel();
//      // creating needed nested groups until the current field (opening tags)
//      for (; currentLevel < (primitiveColumnIO.getFieldPath().length - 1)
//          && d > currentNodePath[currentLevel].getDefinitionLevel(); ++currentLevel) {
//        startGroup(recordConsumer, currentNodePath, currentLevel, primitiveColumnIO);
//      }
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
