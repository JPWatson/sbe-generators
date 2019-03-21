package sbe.generation.csharp;

import org.agrona.generation.OutputManager;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Objects;

import static java.io.File.separatorChar;

/**
 * {@link OutputManager} for managing the creation of Java source files as the target of code generation.
 * The character encoding for the {@link Writer} is UTF-8.
 */
public class CSharpOutputManager implements OutputManager {
    private final File outputDir;

    /**
     * Create a new {@link OutputManager} for generating Java source files into a given package.
     *
     * @param baseDirName for the generated source code.
     * @param packageName for the generated source code relative to the baseDirName.
     */
    public CSharpOutputManager(final String baseDirName, final String packageName) {
        Objects.requireNonNull(baseDirName, "baseDirName");
        Objects.requireNonNull(packageName, "packageName");

        final char lastChar = baseDirName.charAt(baseDirName.length() - 1);
        final String dirName =
                (lastChar == separatorChar ? baseDirName : baseDirName + separatorChar) +
                        packageName.replace('.', separatorChar);

        outputDir = new File(dirName);
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                throw new IllegalStateException("Unable to create directory: " + dirName);
            }
        }
    }

    /**
     * Create a new output which will be a Java source file in the given package.
     * <p>
     * The {@link Writer} should be closed once the caller has finished with it. The Writer is
     * buffer for efficient IO operations.
     *
     * @param name the name of the Java class.
     * @return a {@link Writer} to which the source code should be written.
     */
    public Writer createOutput(final String name) throws IOException {
        final File targetFile = new File(outputDir, name + ".cs");

        return Files.newBufferedWriter(targetFile.toPath(), StandardCharsets.UTF_8);
    }
}
