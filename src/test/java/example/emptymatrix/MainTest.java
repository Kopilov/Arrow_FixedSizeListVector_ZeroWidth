package example.emptymatrix;
import org.junit.jupiter.api.Test;


public class MainTest {
    @Test
    void readIntegerMatrix() throws Exception {
        Main.readIntegerMatrix("build/numericIntMatrix.arrow");
    }

    @Test
    void readIntegerMatrixZeroWidth() throws Exception {
        Main.readIntegerMatrix("build/numericIntMatrixZeroWidth.arrow");
    }

    @Test
    void readSecondColumnAsMatrix() throws Exception {
        Main.readSecondColumnAsMatrix("build/twoColumnsTable.arrow");
    }

    @Test
    void readIntegerMatrixWithNulls() throws Exception {
        Main.readIntegerMatrix("build/numericIntMatrixWithNulls.arrow");
    }

    @Test
    void writeIntegerMatrixWithNulls() throws Exception {
        Main.writeIntegerMatrixWithNulls("build/numericIntMatrixWithNullsFromJava.arrow");
    }
}
