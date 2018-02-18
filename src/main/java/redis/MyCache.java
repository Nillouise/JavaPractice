package redis;

import java.io.Serializable;

/**
 * 因为hessian不要求实现序列化接口，所以参数一律用Object
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public interface MyCache {

    /**
     * @param key
     * @return
     */
    ResultDTO get(Object key);

    ResultDTO put(Object key, Object value);



}
