package com.taobao.tair.etc;

import java.nio.ByteBuffer;
public class CounterPack {
    private Object key = null;
    private int count = 0;
    private int initValue = 0;
    private int expire = 0;

    private static final int encodedSize = 14;

    public CounterPack() {
    }

    public CounterPack(Object key, int count, int initValue, int expire) {
        this.key = key;
        this.count = count;
        this.initValue = initValue;
        this.expire = expire;
    }
    
     

    public CounterPack(Object key, int count, int initValue) {
        this.key = key;
        this.count = count;
        this.initValue = initValue;
    }

    public void encode(ByteBuffer bytes) {
        bytes.putInt(count);
        bytes.putInt(initValue);
        bytes.putInt(expire);
    }

    public static int getEncodedSize() {
        return encodedSize;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getKey() {
        return this.key;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getCount() {
        return this.count;
    }

    public void setInitValue(int initValue) {
        this.initValue = initValue;
    }

    public int getInitValue() {
        return this.initValue;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public int getExpire() {
        return this.expire;
    }

    public String toString() {
        return "KeyValuePack: [\n"
            + "\tkey: " + key + "\n"
            + "\tcount: " + count + "\n"
            + "\tinitValue: " + initValue + "\n"
            + "\texpire: " + expire + "]";
    }
}
