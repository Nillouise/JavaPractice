package redis;

import java.io.Serializable;

/**
 * 暂时采用这个非常烂的实现：缺乏容器类的判空函数
 * 不使用common中的ResultDTO是因为他定成final了，但缺了className和source字段。
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public class CacheResult {
    private Object data;
    private String className;
    //指明这次结果的来源是本机缓存还是redis服务器
    private CacheEnum.Source source;
    private CacheEnum.ErrorCode errorCode;
    private boolean success;

    //工厂模式
    public static CacheResult success(Object value) {
        CacheResult res = new CacheResult();
        res.data = value;
        res.success = true;
        return res;
    }

    public static CacheResult success(Object value,CacheEnum.Source source){
        CacheResult res = new CacheResult();
        res.data = value;
        res.success=true;
        res.source = source;
        return res;
    }

    public static CacheResult fail(CacheEnum.ErrorCode attachMsg){
        CacheResult res = new CacheResult();
        res.errorCode = attachMsg;
        res.success = false;
        return res;
    }

    public CacheEnum.Source getSource() {
        return source;
    }

    public void setSource(CacheEnum.Source source) {
        this.source = source;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public boolean isSuccess()
    {
        return success;
    }

    //禁止默认构造
    private CacheResult() {
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

    public CacheEnum.ErrorCode getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(CacheEnum.ErrorCode errorCode) {
        this.errorCode = errorCode;
    }
}
