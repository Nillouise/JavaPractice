package redis;

import java.io.Serializable;

/**
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public interface MyCache {

    /**
     * @param key
     * @return
     */
    Result get(Serializable key);

    Result put(Serializable key,Serializable value);



}
