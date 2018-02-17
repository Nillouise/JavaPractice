package redis;

import java.io.Serializable;

public class CacheEntry implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public CacheEntry(Serializable data, Status status) {
        super();
        this.data = data;
        this.status = status;
    }

    public Serializable data;
    public Status status;
    public static enum Status { EXIST, NOTEXIST, DELEDTED, DIRTY };
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("DataEntry: ").append(data.toString());
        sb.append("Status: ").append(status);
        return sb.toString();
    }
}
