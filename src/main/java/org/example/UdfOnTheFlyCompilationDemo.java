package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.repeat;

public class UdfOnTheFlyCompilationDemo {

    public static final String UDF_NAME = "custom_repeat";
    public static final String UDF_PACKAGE_NAME = "org.example";
    public static final String UDF_CLASS_NAME = "StringRepeaterUdf";
    public static final String UDF_JAR_PATH = "dynamic-udfs.jar";

    public static void main(String[] args) {
        try {
            // Create compiler instance and prepare UDF compilation info
            JaninoCompilerWrapper compiler = new JaninoCompilerWrapper();
            UdfCompilationInfo uci =
                new UdfCompilationInfo(UDF_NAME, DataTypes.StringType, UDF_PACKAGE_NAME, UDF_CLASS_NAME);

            // Compile UDF source code
            // Resulting bytecode is saved in compiler's internal state
            compiler.compile(uci.getSourceCode());

            // Create jar file with compiled UDF code
            JarCreator jarCreator = new JarCreator();
            jarCreator.createJar(UDF_JAR_PATH, compiler.getClassNameToByteCodeMap());

            // Create simple local SparkSession instance
            SparkSession session = SparkSession
                .builder()
                .master("local[*]")
                .appName("dynamic-udf-compilation-demo")
                .getOrCreate();

            // Adding jar dependency to the current SparkContext to make it available to future tasks
            // Without this, executor processes will fail to load UDF code
            session.sparkContext().addJar(UDF_JAR_PATH);

            // Create UDF instance and register it in the current SparkSession
            Optional<UDF2<String, Integer, String>> udfOpt = compiler.load(uci.getQualifiedClassName());
            if (udfOpt.isPresent()) {
                session.udf().register(uci.getName(), udfOpt.get(), uci.getReturnType());
            } else {
                throw new RuntimeException("Failed to instantiate custom UDF");
            }

            // Sample dataset for UDF showcase
            Dataset<Row> df = sampleDataset(session);
            df.show();

            applyBuiltInRepeatFunction(df);
            applyCustomRepeatUdf(df);

        } catch (Exception ex) {
            // Exception handling
            ex.printStackTrace();
        }
    }

    private static void applyBuiltInRepeatFunction(Dataset<Row> sampleDataFrame) {
        sampleDataFrame
            .withColumn("repeated_builtin", repeat(col("name"), 10))
            .show();
    }

    private static void applyCustomRepeatUdf(Dataset<Row> sampleDataFrame) {
        // Applying registered UDF to a column by leveraging `expr()` or `selectExpr()`
        sampleDataFrame
            .withColumn("repeated_expr", expr("custom_repeat(name, 4)"))
            .show();

        // Applying registered UDF to a column by leveraging `callUDF()`
        sampleDataFrame
            .withColumn("repeated_callUDF", callUDF("custom_repeat", col("name"), lit(4)))
            .show();

        // Calling registered UDF in a SQL query
        sampleDataFrame.createOrReplaceTempView("sample_dataset");
        sampleDataFrame.sparkSession()
            .sql("select custom_repeat(name, 2) as repeated from sample_dataset")
            .show();

        // UDF can be invoked directly as any other function outside any particular DataFrame context
        sampleDataFrame.sparkSession()
            .sql("select custom_repeat('he', 2)")
            .show();
    }

    private static Dataset<Row> sampleDataset(SparkSession session) {
        StructType schema = DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)
            )
        );
        List<Row> rows = Arrays.asList(
            RowFactory.create(1, "a"),
            RowFactory.create(2, "b"),
            RowFactory.create(3, "c"),
            RowFactory.create(4, "d"),
            RowFactory.create(5, "e")
        );
        return session.createDataFrame(rows, schema).cache();
    }
}
