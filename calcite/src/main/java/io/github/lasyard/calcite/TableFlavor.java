package io.github.lasyard.calcite;

import java.util.Locale;
import javax.annotation.Nonnull;

public enum TableFlavor {
    SCANNABLE,
    FILTERABLE,
    PROJECTABLE_FILTERABLE,
    TRANSLATABLE;

    public static TableFlavor of(@Nonnull String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }
}
