package com.taobao.tair.etc;

public class KeyValuePack {
    private Object key = null;
    private Object value = null;
    private short version = 0;
    private int expire = 0;
    private int prefixSize = 0;

    public KeyValuePack() {
    }

    public KeyValuePack(Object key, Object value, short version, int expire) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.expire = expire;
    }

    public KeyValuePack(Object key, Object value, short version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public KeyValuePack(Object key, Object value) {
        this.key = key;
        this.value = value;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getKey() {
        return this.key;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return this.value;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public short getVersion() {
        return this.version;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public int getExpire() {
        return this.expire;
    }

    public void setPrefixSize(int prefixSize) {
        this.prefixSize = prefixSize;
    }

    public int getPrefixSize() {
        return this.prefixSize;
    }

    public String toString() {
        return "KeyValuePack: [\n"
            + "\tkey: " + key + "\n"
            + "\tvalue: " + value + "\n"
            + "\tversion: " + version + "\n"
            + "\texpire: " + expire + "\n"
            + "\tprefixSize: " + prefixSize + "]";
    }
}
