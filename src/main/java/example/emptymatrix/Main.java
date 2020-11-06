package example.emptymatrix;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class Main {
    static FileChannel openFileChannel(File memoryMappingFile) throws IOException {
        return FileChannel.open(memoryMappingFile.toPath(), StandardOpenOption.READ);
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
    }
}
