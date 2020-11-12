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
            for (int j = 0; j < columns; j++) {
                System.out.print(inRow.get(j));
                System.out.print(" ");
            }
            System.out.println();
        }
    }

    static void writeListVector(UnionFixedSizeListWriter writer, List<Integer> values) {
        writer.startList();
        for (Integer v: values) {
            if (v == null) {
                writer.integer().writeNull();
            } else {
                writer.integer().writeInt(v);
            }
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

    static void writeIntegerMatrixWithNulls(String path) throws IOException {
        FixedSizeListVector vector = FixedSizeListVector.empty("numericIntMatrixWithNulls", 4, new RootAllocator());
        UnionFixedSizeListWriter writer = vector.getWriter();
        writer.allocate();

        List<Integer> values1 = Arrays.asList(10, null, 20, 30);
        List<Integer> values2 = Arrays.asList(40, 50, null, 60);
        List<Integer> values3 = Arrays.asList(null, 70, 80, 90);

        //set some values
        writer.setValueCount(3);
        writeListVector(writer, values1);
        writeListVector(writer, values2);
        writeListVector(writer, values3);

        Field field = vector.getField();
        VectorSchemaRoot table = new VectorSchemaRoot(Arrays.asList(field), Arrays.asList(vector));
        saveTable(table, path);
    }

    public static void main(String[] args) {

        try {
            readIntegerMatrix("build/numericIntMatrix.arrow");
        } catch (IOException e) {
            System.err.println("Declared error");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Undeclared error");
            e.printStackTrace();
        }

        try {
            readIntegerMatrix("build/numericIntMatrixZeroWidth.arrow");
        } catch (IOException e) {
            System.err.println("Declared error");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Undeclared error");
            e.printStackTrace();
        }

        try {
            readSecondColumnAsMatrix("build/twoColumnsTable.arrow");
        } catch (IOException e) {
            System.err.println("Declared error");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Undeclared error");
            e.printStackTrace();
        }

        try {
            readIntegerMatrix("build/numericIntMatrixWithNulls.arrow");
        } catch (IOException e) {
            System.err.println("Declared error");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Undeclared error");
            e.printStackTrace();
        }

        try {
            writeIntegerMatrixWithNulls("build/numericIntMatrixWithNullsFromJava.arrow");
        } catch (IOException e) {
            System.err.println("Declared error");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Undeclared error");
            e.printStackTrace();
        }
    }
}
