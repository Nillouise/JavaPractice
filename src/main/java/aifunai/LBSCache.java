package aifunai;

import java.io.Serializable;
import java.util.List;

/**
 * 用户LBS的cache
 * 理论上Cache只存储用户最新的位置应该就能实现lbs对比路书是否偏离的功能了，而不需要存储用户的历史位置。
 * 但可能别的功能需要历史位置，那么是不是需要用一个list存储用户的历史位置？
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-13
 **/
public interface LBSCache {
    /**
     * 在我理解中，保存到Cache里的一切都会变成字符串，因为redis只支持字符串
     * @param keyname
     * @return
     */
    String getLastestVal(String keyname);

    List<String> getAllVal(String keyname);

    /**
     * 此接口应该会清空原本的list的数据
     * @param keyname
     * @param obj     要求serializable 是因为在我理解中，我需要把他转成字符串才能储存到redis里
     * @return
     */
    boolean setVal(String keyname, Serializable obj);

    /**
     * 此接口把数据存到list里
     * @param keyname
     * @param obj
     * @return
     */
    boolean putVal(String keyname,Serializable obj);

    boolean contains(String keyname);

    boolean remove(String keyname);

    /**
     * 让指定key在指定毫秒后失效
     * @param keyname
     * @param millisecond
     * @return 成功设定返回true，失败返回false
     */
    boolean expire(String keyname, long millisecond);
}
