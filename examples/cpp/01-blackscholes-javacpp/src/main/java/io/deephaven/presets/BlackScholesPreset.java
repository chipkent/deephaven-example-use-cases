package io.deephaven.presets;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

/**
 * JavaCPP preset configuration for the Black-Scholes option pricing library.
 * 
 * <p>This class configures JavaCPP to generate Java bindings for the C++ Black-Scholes
 * implementation. JavaCPP will parse the header file and automatically generate:
 * <ul>
 *   <li>Java wrapper classes (io.deephaven.BlackScholes)</li>
 *   <li>JNI bridge code to call the native C++ functions</li>
 * </ul>
 * 
 * <p><b>Configuration Details:</b>
 * <ul>
 *   <li><b>target:</b> "io.deephaven.BlackScholes" - The fully qualified name of the generated Java class</li>
 *   <li><b>includepath:</b> "../../../../shared/blackscholes" - Directory containing the C++ header files</li>
 *   <li><b>include:</b> "blackscholes.h" - The C++ header file to parse for function declarations</li>
 *   <li><b>link:</b> "blackscholes" - The native library name (libblackscholes.so/dylib/dll)</li>
 *   <li><b>@Namespace:</b> "BlackScholes" - Maps C++ BlackScholes namespace to Java class methods</li>
 * </ul>
 * 
 * <p><b>Note:</b> JavaCPP does not follow #include directives in header files. All headers
 * that need to be parsed must be explicitly listed in the include array. The includepath
 * tells JavaCPP where to find these headers.
 * 
 * @see <a href="https://github.com/bytedeco/javacpp">JavaCPP Documentation</a>
 */
@Properties(target="io.deephaven.BlackScholes", value={
  @Platform(
    includepath = "../../../../shared/blackscholes",
    include = "blackscholes.h",
    link = "blackscholes"
  )
})
@Namespace("BlackScholes")
public class BlackScholesPreset implements InfoMapper {
    static {
        Loader.load();
    }

    /**
     * Maps additional configuration for the JavaCPP parser.
     * 
     * <p>This method can be used to provide custom type mappings, skip certain declarations,
     * or add platform-specific configuration. For this simple example, no additional
     * configuration is needed, so the method is empty.
     * 
     * @param infoMap The InfoMap to configure with custom parsing rules
     */
    public void map(InfoMap infoMap) {
        // No custom mappings needed for this simple library
    }
}
