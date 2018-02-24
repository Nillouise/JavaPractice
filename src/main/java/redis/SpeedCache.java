package redis;


import java.io.Serializable;
import java.util.List;

/**
 * 此模块还没有实现作为缓存可以实现的很多接口，如批量put，key的模式匹配，list和set的数据结构，因为目前还没看到有业务需要此类特性，
 * 待有业务需要此类特性时，请通知作者实现 :D
 */

/**
 * 话说key怎么设计比较好（虽然并不是我设计= =）？
 * key没有做模式匹配，也就是说不能查一定模式的多个key
 * 感觉接口的命名不咋样
 * 没有做List、set数据结构的相关接口，后面根据情况再补上
 * key传参为null时，一律返回错误
 * 所有接口均不会抛出异常，一切错误用CacheResult中的errorCode指出（但这有什么优点？感觉抛出异常也不错）
 * 此模块暂时不分子package，因为目测此模块不会涉及业务逻辑，不会膨胀得很大
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public interface SpeedCache {

    /**
     * 此接口会有本地缓存不一致的问题，应该存放不怎么修改，并且不需要expire的数据
     * @param key
     * @return
     */
    CacheResult get(Serializable key);

    /**
     * 批量获取数据，减少RTT
     * 此接口会有本地缓存不一致的问题，应该存放不怎么修改，并且不需要expire的数据
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
     * 给某key增加分数(负值减少分数），这key对应的value应该是一个Integer，CacheResult会返回failure
     */
    CacheResult increseSync(Serializable key,long score);

    /**
     *
     * @param key
     * @return CacheResult isSuccess 为true表示字段存在，为false表示不存在或发生错误
     */
    CacheResult existKeySync(Serializable key);

    CacheResult delKeySync(Serializable keyname);

    /**
     * 让指定key在指定毫秒后失效
     * 注意，就是localcache里的expire信息可能会丢失，从而不会因为expire到期而被删除（从而获取到过时的数据）。
     * 也就是说localCahe里的expire不是那么的可靠，建议被设置expire的数据都从redis里获取
     * 解决此问题需要在每次get后多一次rtt时间，遂搁置
     * @param keyname
     * @param millisecond
     * @return
     */
    CacheResult expireSync(Serializable keyname, long millisecond);

}

