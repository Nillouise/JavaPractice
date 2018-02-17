package aifunai;

import java.io.Serializable;
import java.util.List;

/**
 * PoiInfo的cache
 * 可能需要查经纬度附近的其他Pois的功能？redis做到这个吗？设计一种特殊的keyname？
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-13
 **/
public interface PoiInfoCache {

    /**
     * 在我理解中，保存到Cache里的一切都会变成字符串，因为redis只支持字符串
     * 此接口应该是返回某个特定poi的信息
     * @param keyname
     * @return
     */
    String getVal(String keyname);

    /**
     * 我觉得应该会有同时查一坨不同的poi的需求
     * @param keynames
     * @return
     */
    List<String> getMutiVal(List<String> keynames);

    /**
     * @param keyname
     * @param obj 要求serializable 是因为在我理解中，我需要把他转成字符串才能储存到redis里
     * @return
     */
    boolean setVal(String keyname, Serializable obj);

    boolean contains(String keyname);

    boolean remove(String keyname);

    /**
     * 让指定key在指定毫秒后失效
     *
     * @param keyname
     * @param millisecond
     * @return 设置成功返回true，失败返回false
     */
    boolean expire(String keyname, long millisecond);
}
