package org.example;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompiler;
import org.codehaus.commons.compiler.util.resource.Resource;
import org.codehaus.janino.util.ResourceFinderClassLoader;
import org.codehaus.janino.util.resource.MapResourceCreator;
import org.codehaus.janino.util.resource.MapResourceFinder;
import org.codehaus.janino.util.resource.StringResource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JaninoCompilerWrapper {

    private final Map<String, byte[]> classNameToByteCode = new HashMap<>();
    private final ICompiler compiler;

    public JaninoCompilerWrapper() throws Exception {
        compiler = CompilerFactoryFactory.getDefaultCompilerFactory().newCompiler();
        compiler.setClassFileCreator(new MapResourceCreator(classNameToByteCode));
    }

    public void compile(String sourceCode) {
        try {
            compiler.compile(new Resource[] {
                // Path to source code file is optional and might be empty in our case
                new StringResource("", sourceCode)
            });
        } catch (IOException | CompileException ex) {
            // Exception handling
            ex.printStackTrace();
        }
    }

    public <T> Optional<T> load(String qualifiedClassName) {
        Optional<T> instanceOpt = Optional.empty();
        try {
            ClassLoader classLoader = new ResourceFinderClassLoader(
                new MapResourceFinder(classNameToByteCode),
                ClassLoader.getSystemClassLoader()
            );
            Class<T> clazz = (Class<T>) classLoader.loadClass(qualifiedClassName);
            instanceOpt = Optional.of(clazz.newInstance());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            // Exception handling
            ex.printStackTrace();
        }
        return instanceOpt;
    }

    public Map<String, byte[]> getClassNameToByteCodeMap() {
        return classNameToByteCode;
    }
}
