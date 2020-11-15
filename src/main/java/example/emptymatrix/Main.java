package example.emptymatrix;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

public class Main {
    static FileChannel openFileChannel(File memoryMappingFile) throws IOException {
        return FileChannel.open(memoryMappingFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    static ArrowFileReader initReader(String fileName) throws IOException {
        ArrowFileReader reader = new ArrowFileReader(openFileChannel(new File(fileName)), new RootAllocator());
        reader.initialize();
        return reader;
    }

    static VectorSchemaRoot readTable(String fileName) throws IOException {
        ArrowFileReader reader = initReader(fileName);
        reader.loadNextBatch();
        return reader.getVectorSchemaRoot();
    }

    static void saveTable(VectorSchemaRoot table, String fileName) throws IOException {
        ArrowFileWriter writer = new ArrowFileWriter(table, null, openFileChannel(new File(fileName)));
        writer.start();
        writer.writeBatch();
        writer.end();
        writer.close();
    }

    private static void printMatrix(FixedSizeListVector vector) {
        int rows = vector.getValueCount();
        int columns = vector.getListSize();
        System.out.println("Size is " + rows + " * " + columns);
        for (int i = 0; i < rows; i++) {
            List<?> inRow = (List<?>) vector.getObject(i);
            if (inRow == null) {
                System.out.println("nullrow");
                continue;
            }
            for (int j = 0; j < columns; j++) {
                System.out.print(inRow.get(j));
                System.out.print(" ");
            }
            System.out.println();
        }
    }

    static void writeListVector(UnionFixedSizeListWriter writer, FixedSizeListVector vector, List<Integer> values) {
        writer.startList();
        if (values != null) {
            for (Integer v: values) {
                if (v == null) {
                    writer.integer().writeNull();
                } else {
                    writer.integer().writeInt(v);
                }
            }
        } else {
            vector.setNull(writer.getPosition());
        }
        writer.endList();
    }

    static void readIntegerMatrix(String path) throws IOException {
        VectorSchemaRoot table = readTable(path);
        FixedSizeListVector vector = (FixedSizeListVector) table.getVector(0);
        printMatrix(vector);
    }

    static void readSecondColumnAsMatrix(String path) throws IOException {
        VectorSchemaRoot table = readTable(path);
        FixedSizeListVector vector = (FixedSizeListVector) table.getVector(1);
        printMatrix(vector);
    }

    static void writeIntegerMatrix(String path) throws IOException {
        FixedSizeListVector vector = FixedSizeListVector.empty("numericIntMatrixWithNullRows", 4, new RootAllocator());
        UnionFixedSizeListWriter writer = vector.getWriter();
        writer.allocate();

        List<Integer> values1 = Arrays.asList(10, null, 20, 30);
        List<Integer> values2 = Arrays.asList(40, 50, null, 60);
        List<Integer> values3 = null;
        List<Integer> values4 = Arrays.asList(null, 70, 80, 90);

        //set some values
        writer.setValueCount(4);
        writeListVector(writer, vector, values1);
        writeListVector(writer, vector, values2);
        writeListVector(writer, vector, values3);
        writeListVector(writer, vector, values4);

        Field field = vector.getField();
        VectorSchemaRoot table = new VectorSchemaRoot(Arrays.asList(field), Arrays.asList(vector));
        saveTable(table, path);
    }

    static void writeIntegerMatrixZeroWidth(String path) throws IOException {
        FixedSizeListVector vector = FixedSizeListVector.empty("zeroWidthMatrixWithNullRows", 0, new RootAllocator());
        UnionFixedSizeListWriter writer = vector.getWriter();
        writer.allocate();

        List<Integer> values1 = new ArrayList<>();
        List<Integer> values2 = new ArrayList<>();
        List<Integer> values3 = null;
        List<Integer> values4 = new ArrayList<>();

        //set some values
        writer.setValueCount(4);
        writeListVector(writer, vector, values1);
        writeListVector(writer, vector, values2);
        writeListVector(writer, vector, values3);
        writeListVector(writer, vector, values4);

        Field field = vector.getField();
        VectorSchemaRoot table = new VectorSchemaRoot(Arrays.asList(field), Arrays.asList(vector));
        saveTable(table, path);
    }

    public static void main(String[] args) {

        List<Callable> cases = Arrays.asList(
                () -> {readIntegerMatrix("build/numericIntMatrix.arrow"); return null;},
                () -> {readIntegerMatrix("build/numericIntMatrixZeroWidth.arrow"); return null;},
                () -> {readSecondColumnAsMatrix("build/twoColumnsTable.arrow"); return null;},
                () -> {writeIntegerMatrix("build/numericIntMatrixFromJava.arrow"); return null;},
                () -> {writeIntegerMatrixZeroWidth("build/numericIntMatrixZeroWidthFromJava.arrow"); return null;}
        );

        for (Callable testCase: cases) {
            try {
                testCase.call();
            } catch (IOException e) {
                System.err.println("Declared error");
                e.printStackTrace();
            } catch (Exception e) {
                System.err.println("Undeclared error");
                e.printStackTrace();
            }
        }
    }
}
