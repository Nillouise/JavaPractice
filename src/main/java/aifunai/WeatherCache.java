package aifunai;

import java.io.Serializable;

/**
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-13
 **/
public interface WeatherCache {


    String getVal(String keyname);

    boolean setVal(String keyname, Serializable obj);

    boolean contains(String keyname);

    boolean remove(String keyname);

    /**
     * 让指定key在指定毫秒后失效
     * @param keyname
     * @param millisecond
     * @return 成功设定返回true，失败返回false
     */
    boolean expire(String keyname,long millisecond);
}
