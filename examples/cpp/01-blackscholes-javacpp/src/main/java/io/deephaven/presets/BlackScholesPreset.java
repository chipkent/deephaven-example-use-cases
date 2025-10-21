package io.deephaven.presets;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(target="io.deephaven.BlackScholes", value={
  @Platform(
    includepath = "../../../../shared/blackscholes",
    include = "blackscholes.h",
    link = "blackscholes"
  )
})
public class BlackScholesPreset implements InfoMapper {
    static {
        Loader.load();
    }

    public void map(InfoMap infoMap) {
    }
}
