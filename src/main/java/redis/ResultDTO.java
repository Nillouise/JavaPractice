package redis;

import java.io.Serializable;

/**
 * 暂时采用这个非常烂的实现：固定类型而不采用泛型，缺乏容器类的判空函数，没有定下错误码
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public class ResultDTO {
    private Object data;
    private String className;
    //指明这次结果的来源是本机缓存还是redis服务器
    private String source;
    private MyConstant attachMsg;
    private boolean status;

    //工厂模式
    public static ResultDTO success(Object value) {
        ResultDTO res = new ResultDTO();
        res.data = value;
        res.status = true;
        return res;
    }

    public static ResultDTO fail(MyConstant attachMsg){
        ResultDTO res = new ResultDTO();
        res.attachMsg = attachMsg;
        res.status = false;
        return res;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public boolean isSuccess()
    {
        return status;
    }

    //禁止默认构造
    private ResultDTO() {
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public MyConstant getAttachMsg() {
        return attachMsg;
    }

    public void setAttachMsg(MyConstant attachMsg) {
        this.attachMsg = attachMsg;
    }
}
