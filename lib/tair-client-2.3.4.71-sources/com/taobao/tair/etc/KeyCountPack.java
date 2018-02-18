package com.taobao.tair.etc;

public class KeyCountPack {
    private Object key = null;
    private int count = 0;
    private short version = 0;
    private int expire = 0;
    private int prefixSize = 0;

    public KeyCountPack() {
    }

    public KeyCountPack(Object key, int count, short version, int expire) {
        this.key = key;
        this.count = count;
        this.version = version;
        this.expire = expire;
    }

    public KeyCountPack(Object key, int count, short version) {
        this.key = key;
        this.count = count;
        this.version = version;
    }

    public KeyCountPack(Object key, int count) {
        this.key = key;
        this.count = count;
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
        return "KeycountPack: [\n"
            + "\tkey: " + key + "\n"
            + "\tcount: " + count + "\n"
            + "\tversion: " + version + "\n"
            + "\texpire: " + expire + "\n"
            + "\tprefixSize: " + prefixSize + "]";
    }
}
