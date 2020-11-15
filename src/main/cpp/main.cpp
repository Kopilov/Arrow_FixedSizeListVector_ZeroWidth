#include <iostream>
#include <sys/stat.h>
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"

void exitOnError(arrow::Status status) {
    if (!status.ok()) {
        std::cerr << status.message() << std::endl;
        std::terminate();
    }
}

inline bool file_exists (const std::string& name) {
    struct stat buffer;
    return (stat (name.c_str(), &buffer) == 0);
}

std::shared_ptr<arrow::FixedSizeListArray> generateIntegerMatrix(std::shared_ptr<arrow::DataType>* type) {
    std::shared_ptr<arrow::FixedSizeListArray> array;
    std::shared_ptr<arrow::NumericBuilder<arrow::Int32Type>> nestedBuilder = std::make_shared<arrow::NumericBuilder<arrow::Int32Type>>();
    std::shared_ptr<arrow::FixedSizeListBuilder> builder = std::make_shared<arrow::FixedSizeListBuilder>(arrow::default_memory_pool(), nestedBuilder, 4);
    arrow::Status status;

    status = builder->Append();
    exitOnError(status);
    status = nestedBuilder->AppendValues({10, 1, 20, 30}, {true, false, true, true});
    exitOnError(status);
    status = builder->Append();
    exitOnError(status);
    status = nestedBuilder->AppendValues({40, 50, 2, 60}, {true, true, false, true});
    exitOnError(status);
    status = builder->AppendNull();
    exitOnError(status);
    status = builder->Append();
    exitOnError(status);
    status = nestedBuilder->AppendValues({3, 70, 80, 90}, {false, true, true, true});
    exitOnError(status);

    status = builder->Finish(&array);
    exitOnError(status);
    *type = builder->type();
    return array;
}

std::shared_ptr<arrow::FixedSizeListArray> generateIntegerMatrixZeroWidth(std::shared_ptr<arrow::DataType>* type) {
    std::shared_ptr<arrow::FixedSizeListArray> array;
    std::shared_ptr<arrow::NumericBuilder<arrow::Int32Type>> nestedBuilder = std::make_shared<arrow::NumericBuilder<arrow::Int32Type>>();
    std::shared_ptr<arrow::FixedSizeListBuilder> builder = std::make_shared<arrow::FixedSizeListBuilder>(arrow::default_memory_pool(), nestedBuilder, 0);
    arrow::Status status;

    status = builder->Append();
    exitOnError(status);
    status = nestedBuilder->AppendValues({});
    exitOnError(status);
    status = builder->Append();
    exitOnError(status);
    status = nestedBuilder->AppendValues({});
    exitOnError(status);
    status = builder->AppendNull();
    exitOnError(status);
    status = builder->Append();
    exitOnError(status);
    status = nestedBuilder->AppendValues({});
    exitOnError(status);

    status = builder->Finish(&array);
    exitOnError(status);

    *type = builder->type();
    return array;
}

void writeOneColumnTable(std::function<std::shared_ptr<arrow::Array>(std::shared_ptr<arrow::DataType>*)>generateArray, std::string columnName, std::string fileName) {
    std::cout << "write " << fileName << std::endl;
    std::shared_ptr<arrow::DataType> type;
    std::shared_ptr<arrow::Array> array = generateArray(&type);


    std::shared_ptr<arrow::Field> fieldExample = field(columnName, type);
    std::shared_ptr<arrow::Schema> schemaExample = arrow::schema({fieldExample});
    std::shared_ptr<arrow::Table> tableExample = arrow::Table::Make(schemaExample, {array});

    std::shared_ptr<arrow::io::FileOutputStream> openedFile = *(arrow::io::FileOutputStream::Open(fileName));
    arrow::Status status = arrow::ipc::feather::WriteTable(*tableExample, &(*openedFile));
    exitOnError(status);
}

void writeTwoColumnsTable(std::string fileName) {
    std::cout << "write " << fileName << std::endl;
    std::shared_ptr<arrow::DataType> type;

    std::shared_ptr<arrow::Array> arrayIntEmpty = generateIntegerMatrixZeroWidth(&type);
    std::shared_ptr<arrow::Field> fieldIntEmpty = field("NumericIntMatrixZeroWidthColumn", type);

    std::shared_ptr<arrow::Array> arrayInt = generateIntegerMatrix(&type);
    std::shared_ptr<arrow::Field> fieldInt = field("NumericIntMatrixColumn", type);

    std::shared_ptr<arrow::Schema> schemaExample = arrow::schema({fieldIntEmpty, fieldInt});
    std::shared_ptr<arrow::Table> tableExample = arrow::Table::Make(schemaExample, {arrayIntEmpty, arrayInt});

    std::shared_ptr<arrow::io::FileOutputStream> openedFile = *(arrow::io::FileOutputStream::Open(fileName));
    arrow::Status status = arrow::ipc::feather::WriteTable(*tableExample, &(*openedFile));
    exitOnError(status);
}

void printMatrix(std::shared_ptr<arrow::FixedSizeListArray> list_array) {
    std::cout << "Size is " << list_array->length() << " * " << list_array->list_type()->list_size() << std::endl;

    for (int i = 0; i < list_array->length(); i++) {
        if (list_array->IsNull(i)) {
            std::cout << "nullrow" << std::endl;
            continue;
        }
        std::shared_ptr<arrow::Int32Array> list = std::static_pointer_cast<arrow::Int32Array>(list_array->value_slice(i));
        for (int j = 0; j < list->length(); j++) {
            if (list->IsValid(j)) {
                std::cout << list->Value(j) << " ";
            } else {
                std::cout << "null" << " ";
            }
        }
        std::cout << std::endl;
    }
}

void readIntegerMatrix(std::string fileName) {
    std::shared_ptr<arrow::ipc::feather::Reader> reader = (*arrow::ipc::feather::Reader::Open(
                                                               *(arrow::io::MemoryMappedFile::Open(fileName, arrow::io::FileMode::READ)))
                                                           );
    std::shared_ptr<arrow::Table> tableTarget;
    arrow::Status status = reader->Read(&tableTarget);
    exitOnError(status);

    std::shared_ptr<arrow::Array> array = tableTarget->column(0)->chunk(0);
    std::shared_ptr<arrow::FixedSizeListArray> list_array = std::static_pointer_cast<arrow::FixedSizeListArray>(array);
    printMatrix(list_array);
}

void readSecondColumnAsMatrix(std::string fileName) {
    std::shared_ptr<arrow::ipc::feather::Reader> reader = (*arrow::ipc::feather::Reader::Open(
                                                               *(arrow::io::MemoryMappedFile::Open(fileName, arrow::io::FileMode::READ)))
                                                           );
    std::shared_ptr<arrow::Table> tableTarget;
    arrow::Status status = reader->Read(&tableTarget);
    exitOnError(status);

    std::shared_ptr<arrow::Array> array = tableTarget->column(1)->chunk(0);
    std::shared_ptr<arrow::FixedSizeListArray> list_array = std::static_pointer_cast<arrow::FixedSizeListArray>(array);
    printMatrix(list_array);
}

int main(int argsc, char** argsv) {
    if (file_exists("numericIntMatrixFromJava.arrow")) {
        readIntegerMatrix("numericIntMatrix.arrow");
        readIntegerMatrix("numericIntMatrixZeroWidth.arrow");
        readSecondColumnAsMatrix("twoColumnsTable.arrow");
        readIntegerMatrix("numericIntMatrixFromJava.arrow");
        readIntegerMatrix("numericIntMatrixZeroWidthFromJava.arrow");
    } else {
        try {
            writeOneColumnTable(generateIntegerMatrix, "NumericIntMatrix", "numericIntMatrix.arrow");
            writeOneColumnTable(generateIntegerMatrixZeroWidth, "NumericIntMatrixZeroWidth", "numericIntMatrixZeroWidth.arrow");
            writeTwoColumnsTable("twoColumnsTable.arrow");
        } catch(std::exception& e) {
            std::terminate();
        }
    }

}
