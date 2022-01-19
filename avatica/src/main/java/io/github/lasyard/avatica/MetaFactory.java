package io.github.lasyard.avatica;

import org.apache.calcite.avatica.Meta;

import java.util.List;

// Used by HttpServer
@SuppressWarnings("unused")
public class MetaFactory implements Meta.Factory {
    @Override
    public Meta create(List<String> args) {
        return new ServerMeta();
    }
}
