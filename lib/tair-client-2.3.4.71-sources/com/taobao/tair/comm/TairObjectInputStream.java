package com.taobao.tair.comm;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.concurrent.ConcurrentHashMap;


public class TairObjectInputStream extends ObjectInputStream {

    private final static ConcurrentHashMap<ClassLoader, ConcurrentHashMap<String, Class>> loader2Cache = new ConcurrentHashMap<ClassLoader, ConcurrentHashMap<String, Class>>();

    private ClassLoader classLoader = null;

    public TairObjectInputStream(InputStream in, ClassLoader classLoader)
            throws IOException {
        super(in);
        this.classLoader = classLoader;
    }


    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException {
        if (classLoader == null) {
            classLoader = TairObjectInputStream.class.getClassLoader();
        }

        try {
            String name = desc.getName();
            ConcurrentHashMap<String, Class> cache = loader2Cache.get(classLoader);
            if (cache == null) {
                cache = new ConcurrentHashMap<String, Class>();
                ConcurrentHashMap<String, Class> old = loader2Cache.putIfAbsent(classLoader, cache);
                if (old != null) {
                    cache = old;
                }
            }
            Class clazz = cache.get(name);
            if (clazz == null) {
                clazz = Class.forName(name, false, classLoader);
                cache.putIfAbsent(name, clazz);
            }
            return clazz;
        } catch (ClassNotFoundException e) {
            return super.resolveClass(desc);
        }
    }

}