package redis;

import java.io.Serializable;

/**
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public class Result {
    private Serializable value;
    private String className;
    private RedisConstant attachMsg;

    public Serializable getValue() {
        return value;
    }

    public void setValue(Serializable value) {
        this.value = value;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public RedisConstant getAttachMsg() {
        return attachMsg;
    }

    public void setAttachMsg(RedisConstant attachMsg) {
        this.attachMsg = attachMsg;
    }
}
