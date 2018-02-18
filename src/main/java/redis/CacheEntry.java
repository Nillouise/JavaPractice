package redis;

import java.io.Serializable;

//这个CacheEntry 应该只是只会存在localcache中，跟其他服务的交互时会转换成其他形式
public class CacheEntry{
    public CacheEntry(byte[] data, Status status) {
        super();
        this.data = data;
        this.status = status;
    }

    //这里用个泛型可能会比较好
    private byte[] data;
    private Status status;

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public static enum Status {EXIST, NOTEXIST, DELEDTED, DIRTY}

    ;

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("DataEntry: ").append(data.toString());
        sb.append("Status: ").append(status);
        return sb.toString();
    }
}
