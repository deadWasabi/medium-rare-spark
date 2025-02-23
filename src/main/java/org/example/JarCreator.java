package org.example;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class JarCreator {

    public void createJar(String path, Map<String, byte[]> classNameToByteCodeMap) {
        byte[] jarContent = generateJarContent(classNameToByteCodeMap);
        write(path, jarContent);
    }

    private byte[] generateJarContent(Map<String, byte[]> classNameToByteCodeMap) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JarOutputStream jos = new JarOutputStream(baos)) {
            for (String classFilePath : classNameToByteCodeMap.keySet()) {
                jos.putNextEntry(new JarEntry(classFilePath));
                jos.write(classNameToByteCodeMap.get(classFilePath));
                jos.closeEntry();
            }
        } catch (IOException ex) {
            // Exception handling
            ex.printStackTrace();
        }
        return baos.toByteArray();
    }

    // Alternatively jar can be written to HDFS or other locations
    private void write(String path, byte[] jarContent) {
        try(FileOutputStream outputStream = new FileOutputStream(path)) {
            outputStream.write(jarContent);
        } catch (IOException ex) {
            // Exception handling
            ex.printStackTrace();
        }
    }
}
