package redis;


import java.io.Serializable;
import java.util.List;

/**
 * 此模块暂时不分子package，因为目测此模块不会涉及业务逻辑，不会膨胀得很大
 */

/**
 * 因为hessian不要求实现序列化接口，所以参数一律用Object，话说key怎么设计比较好（虽然并不是我设计= =）？
 * key没有做模式匹配，也就是说不能查一定模式的多个key
 * 感觉接口的命名不咋样
 * 没有做List、set数据结构的相关接口，后面根据情况再补上
 * key传参为null，一律返回错误
 * 所有接口均不会抛出异常，一切错误用CacheResult中的errorCode指出
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public interface SpeedCache {

    /**
     * 此接口会有本地缓存不一致的问题，应该存放不怎么修改的数据
     * @param key
     * @return
     */
    CacheResult get(Serializable key);

    /**
     * 批量获取数据，减少RTT
     * 此接口会有本地缓存不一致的问题，应该存放不怎么修改的数据
     * @param keys
     * @return
     */
    List<CacheResult> getBatch(List<?extends Serializable> keys);

    /**
     * 此接口直接从redis获取数据，不会经过本地缓存，不存在不一致的问题。
     * @param key
     * @return
     */
    CacheResult getByRedis(Serializable key);

    /**
     * 批量获取数据，减少RTT
     * 此接口直接从redis获取数据，不会经过本地缓存，不存在不一致的问题。
     * @param keys
     * @return
     */
    List<CacheResult> getByRedisBatch(List<?extends Serializable> keys);

    /**
     * 此接口同步设置数据，执行时间可能较长
     * @param key
     * @param value
     * @return
     */
    CacheResult setSync(Serializable key, Serializable value);

    /**
     * 给某key增加分数(负值减少分数），这key对应的value应该是一个Integer
     */
    CacheResult increseSync(Serializable key,long score);

    CacheResult existKeySync(Serializable key);

    CacheResult delKeySync(Serializable keyname);

    /**
     * 让指定key在指定毫秒后失效
     * @param keyname
     * @param millisecond
     */
    CacheResult expireSync(Serializable keyname, long millisecond);

}

