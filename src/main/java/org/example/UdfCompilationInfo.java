package org.example;

import org.apache.spark.sql.types.DataType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class UdfCompilationInfo {

    public static final String UDF_TEMPLATE_PATH = "UdfTemplate.java.template";

    private final String name;
    private final DataType returnType;
    private final String qualifiedClassName;
    private final String sourceCode = readUdfTemplate();

    public UdfCompilationInfo(String name, DataType returnType, String packageName, String className) {
        this.name = name;
        this.returnType = returnType;
        this.qualifiedClassName = String.join(".", packageName, className);
    }

    public String getName() {
        return name;
    }

    public String getSourceCode() {
        return sourceCode;
    }

    public DataType getReturnType() {
        return returnType;
    }

    public String getQualifiedClassName() {
        return qualifiedClassName;
    }

    private String readUdfTemplate() {
        StringBuilder sb = new StringBuilder();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(UDF_TEMPLATE_PATH)) {
            if (inputStream != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                sb.append(reader.lines().collect(Collectors.joining(System.lineSeparator())));
            }
        } catch (IOException ex) {
            // Exception handling
            ex.printStackTrace();
        }
        return sb.toString();
    }
}
