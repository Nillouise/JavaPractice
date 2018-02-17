package aifunai;

import java.io.Serializable;
import java.util.List;

/**
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-13
 **/
public interface TagInfoCache {
    /**
     * 在我理解中，保存到Cache里的一切都会变成字符串，因为redis只支持字符串
     * @param userTagName 这个userTagName 应该是某个用户的某个tag的记录名吧
     * @return
     */
    String getVal(String userTagName);

    /**
     * 应该会有给标签增加减少分数的功能吧
     * @param userTagName 这个userTagName应该是某个用户的某个tag的记录名吧
     */
    boolean decrese(String userTagName,Long score);

    /**
     * 应该会有给标签增加分数的功能吧
     * @param userTagName 这个userTagName 应该是某个用户的某个tag的记录名吧
     *
     */
    boolean increse(String userTagName,Long score);

    /**
     * 返回一个user的所有tag
     * @param userName
     * @return
     */
    List<String> getTags(String userName);

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
     *
     * @param keyname
     * @param millisecond
     * @return 成功设定返回true，失败返回false
     */
    boolean expire(String keyname, long millisecond);
}
