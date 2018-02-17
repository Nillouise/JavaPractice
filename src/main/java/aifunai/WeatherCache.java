package aifunai;

import java.io.Serializable;

/**
 * 理论上这应该是指存储天气变坏的数据，不会存储好天气
 *
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-13
 **/
public interface WeatherCache {

    /**
     * 在我理解中，保存到Cache里的一切都会变成字符串，因为redis只支持字符串
     *
     * @param keyname
     * @return
     */
    String getVal(String keyname);

    /**
     * @param keyname
     * @param obj     要求serializable 是因为在我理解中，我需要把他转成字符串才能储存到redis里
     * @return
     */
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
