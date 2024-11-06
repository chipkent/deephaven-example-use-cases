// file: src/main/java/org/example/presets/HelloPreset.java
package org.example.presets;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(target="org.example.hello", value={
  @Platform(include = "myhello.h", link = "myhello")
})
public class HelloPreset implements InfoMapper {
    static {
        Loader.load();
    }

    public void map(InfoMap infoMap) {
    }
}
