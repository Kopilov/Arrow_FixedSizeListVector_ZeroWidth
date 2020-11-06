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
}
