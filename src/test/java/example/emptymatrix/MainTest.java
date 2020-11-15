package example.emptymatrix;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNull;


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
    void writeIntegerMatrix() throws Exception {
        Main.writeIntegerMatrix("build/numericIntMatrixFromJava.arrow");
    }

    @Test
    void writeIntegerMatrixZeroWidth() throws Exception {
        Main.writeIntegerMatrixZeroWidth("build/numericIntMatrixZeroWidthFromJava.arrow");
    }
}
