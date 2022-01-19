package io.github.lasyard.avatica.server;

import org.apache.calcite.avatica.server.Main;

import java.lang.reflect.InvocationTargetException;

public final class Entry {
    private Entry() {
    }

    public static void main(String[] args) throws
        InterruptedException,
        ClassNotFoundException,
        InvocationTargetException,
        IllegalAccessException,
        InstantiationException,
        NoSuchMethodException {
        Main.main(new String[]{
            "io.github.lasyard.avatica.MetaFactory"
        });
    }
}
