package io.github.lasyard.avatica.local;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

class LocalStatement extends AvaticaStatement {
    LocalStatement(
        LocalConnection connection,
        Meta.StatementHandle handle,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) {
        super(
            connection,
            handle,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }

    // `getSignature` is protected, so bridge it.
    Meta.CursorFactory getCursorFactory() {
        return getSignature().cursorFactory;
    }

    // `setSignature` is protected, so bridge it.
    void setMetaSignature(Meta.Signature signature) {
        setSignature(signature);
    }
}
