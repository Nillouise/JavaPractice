package redis;

import java.io.*;
import java.util.*;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import sun.misc.Cache;

import javax.xml.transform.Result;

/**
 * linkedHashMap不是线程安全的，恐怕要我自己手动加锁了，但直接加锁感觉性能不咋样
 * <p>
 * 经纬度范围查询数据 不做在缓存层
 * 经常修改会导致多个localCache不一致的情况
 * TODO：功能：启动的配置信息，启动载入数据，超时控制，高级数据结构利用，经纬度范围查询数据，localCache与redis一致性，多线程，合适的错误处理
 * 数据倾斜，乐观锁，数据落入mysql、hbase等等
 * 多台机器上localCache之间的数据需要同步吗
 * 做了的功能：LRU缓存，序列压缩数据。
 *
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public class SpeedCacheImpl implements SpeedCache {

    final private LRULocalCache<Serializable, CacheEntry> localCache;
    private int localCacheSize;
    private String remoteServerUrl;
    private JedisPool jedisPool;

    //这里使用build模式配置SpeedCache会比较好
    public SpeedCacheImpl(int localCacheSize) {
        this.localCacheSize = localCacheSize;
        this.localCache = new LRULocalCache<>(16, 0.75f, true, localCacheSize);
        this.remoteServerUrl = "localhost";
        //启动检查是否连接成功，以及自动尝试连接一定次数。
        jedisPool = new JedisPool(new JedisPoolConfig(), remoteServerUrl);
    }

    //目前还没查清jedis会怎么抛出异常
    //jedis.get会返回null
    private byte[] redisGet(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        }
    }

    private Long redisDelKey(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key);
        }
    }

    private Long redisExpire(byte[] key, long millisecond) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.pexpire(key, millisecond);
        }
    }

    public void test_redisGet() {
        byte[] bytes = redisGet(new byte[0]);
        System.out.println(bytes);
    }


    /**
     * @param keys   不能为null,而且我不会给你检查keys是否为null
     * @param result 用来接收结果的列表
     * @return localCache中key没有命中的数目
     */
    private int localGetBatch(List<? extends Serializable> keys, List<CacheEntry> result) {
        int nullcnt = 0;
        Date now = new Date();
        synchronized (localCache) {
            for (Serializable key : keys) {
                CacheEntry entry = localCache.get(key);
                if (entry == null || entry.getExpireTime() < now.getTime()) {
                    nullcnt++;
                    entry = null;
                }
                result.add(entry);
            }
            return nullcnt;
        }
    }

    /**
     * 注意mget这函数会返回‘nil’作为某个key的空值，具体还没测试
     * TODO：需要测试
     *
     * @param keys 不能为null
     * @return
     */
    private List<byte[]> redisGetBatch(List<byte[]> keys) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.mget(keys.toArray(new byte[keys.size()][]));
        }
    }

    private String redisPut(byte[] key, byte[] val) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.set(key, val);
        }
    }

    private Boolean redisExistsKey(byte[] key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(key);
        }
    }

    private CacheEntry localGet(Serializable key) {
        synchronized (localCache) {
            return localCache.get(key);
        }
    }

    private void localSet(Serializable key, CacheEntry val) {
        synchronized (localCache) {
            localCache.put(key, val);
        }
    }

    private void localRemove(Serializable key) {
        synchronized (localCache) {
            localCache.remove(key);
        }
    }

    private static byte[] serializable(Serializable value) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        HessianOutput ho = new HessianOutput(os);
        ho.writeObject(value);
        return os.toByteArray();
    }

    private static Object deserialize(byte[] by) throws IOException {
        if (by == null) throw new NullPointerException();
        ByteArrayInputStream is = new ByteArrayInputStream(by);
        HessianInput hi = new HessianInput(is);
        return hi.readObject();
    }

//    //不知道这个优化是否有必要。//经测试，性能比一个一个地转换还差
//    @Deprecated
//    private static List<byte[]> serializable(List<? extends Serializable> values) throws IOException {
//        ByteArrayOutputStream os = new ByteArrayOutputStream();
//        HessianOutput ho = new HessianOutput(os);
//
//        List<byte[]> res = new ArrayList<>(values.size());
//
//        for (Serializable value : values) {
//            os.flush();
//            ho.writeObject(value);
//            res.add(os.toByteArray());
//        }
//        return res;
//    }

//    private static void test_serializable() throws IOException {
//        List<Serializable> values = new ArrayList<>();
//        for (int i = 0; i < 10000; i++) {
//            values.add(String.valueOf(i));
//        }
//
//        //调用语句的顺序会影响后面语句的速度的
//        long preTime = System.currentTimeMillis();
//        serializable(values);
//        System.out.printf("serializable batch cost time: %d\n", System.currentTimeMillis() - preTime);
//
//
//        preTime = System.currentTimeMillis();
//        for (Serializable value : values) {
//            serializable(value);
//        }
//        System.out.printf("serializable one by one cost time: %d\n", System.currentTimeMillis() - preTime);
//    }

    private CacheResult noExistRedis(Serializable key) {
        localRemove(key);
        return CacheResult.fail(CacheEnum.ErrorCode.NO_EXIST_REDIS);
    }

    @Override
    public CacheResult get(Serializable key) {
        if (key == null) {
            return CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER);
        }
        try {
            //在本地缓存找
            CacheEntry cacheEntry = localGet(key);
            Date now = new Date();
            //处理expire的key
            if (cacheEntry != null && cacheEntry.getExpireTime() < now.getTime()) {
                localRemove(key);
                cacheEntry = null;
            }
            if (cacheEntry != null && CacheEntry.Status.EXIST.equals(cacheEntry.getStatus())
                    && (cacheEntry.getExpireTime() == null || cacheEntry.getExpireTime() >= now.getTime())) {
                CacheResult cacheResult = CacheResult.success(deserialize(cacheEntry.getData()));
                //这里写上本机服务器地址或者名字比较好
                cacheResult.setSource(CacheEnum.Source.LOCAL_CACHE);
                return cacheResult;
            } else {
                //这里开始上redis找缓存
                byte[] bKey = serializable(key);
                byte[] response = redisGet(bKey);
                if (response == null) {
                    return noExistRedis(key);
                }
                CacheResult success = CacheResult.success(deserialize(response));
                success.setSource(CacheEnum.Source.REDIS);
                return success;
            }
        } catch (IOException ioe) {
            return CacheResult.fail(CacheEnum.ErrorCode.IO_EXCEPTION);
        } catch (Exception e) {
            return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
        }
    }

    @Override
    public List<CacheResult> getBatch(List<? extends Serializable> keys) {
        if (keys == null) {
            List<CacheResult> res = new ArrayList<>();
            res.add(CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER));
            return res;
        }
        try {
            List<CacheEntry> localCache = new ArrayList<>();
            int nullcnt = localGetBatch(keys, localCache);
            if (nullcnt == 0) {
                List<CacheResult> res = new ArrayList<>();
                for (CacheEntry entry : localCache) {
                    if (entry == null) {
                        res.add(CacheResult.fail(CacheEnum.ErrorCode.LOCAL_CACHE_ERROR));
                    } else {
                        res.add(CacheResult.success(deserialize(entry.getData())));
                    }
                }
                return res;
            } else {
                //当localcache有一个以上未命中的时候，全部数据直接从redis里取一遍，因为rtt比执行时间要大得多。
                return getByRedisBatch(keys);
            }
        } catch (Exception e) {
            List<CacheResult> res = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                res.add(CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR));
            }
            return res;
        }
    }

    public List<CacheResult> getBatch(Serializable... keys) {
        return getBatch(Arrays.asList(keys));
    }

    @Override
    public CacheResult getByRedis(Serializable key) {
        if(key==null){
            return CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER);
        }
        try {
            byte[] bytes = redisGet(serializable(key));
            if (bytes == null) {
                return noExistRedis(key);
            }
            CacheEntry e = new CacheEntry(bytes, CacheEntry.Status.EXIST);
            localSet(key, e);
            return CacheResult.success(deserialize(bytes));
        } catch (Exception e) {
            return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
        }
    }

    @Override
    public List<CacheResult> getByRedisBatch(List<? extends Serializable> keys) {
        if (keys == null) {
            List<CacheResult> res = new ArrayList<>();
            res.add(CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER));
            return res;
        }
        try {
            List<byte[]> redisKeys = new ArrayList<>();
            for (Serializable key : keys) {
                try {
                    redisKeys.add(serializable(key));
                } catch (IOException e) {
                    redisKeys.add(new byte[0]);
                }
            }
            List<CacheResult> res2 = new ArrayList<>();
            //TODO：要是redis返回value的数量跟keys的数量不一致怎么办？
            List<byte[]> response = redisGetBatch(redisKeys);
            //刷新到本地缓存
            for (int i = 0; i < keys.size(); i++) {
                if (response.get(i) == null) {
                    localRemove(keys.get(i));
                }
                CacheEntry c = new CacheEntry(response.get(i), CacheEntry.Status.EXIST);
                localSet(keys.get(i), c);
            }
            //构造返回值
            for (byte[] aByte : response) {
                if (aByte == null) {
                    res2.add(CacheResult.fail(CacheEnum.ErrorCode.NO_EXIST_REDIS));
                } else {
                    res2.add(CacheResult.success(aByte, CacheEnum.Source.REDIS));
                }
            }
            return res2;
        } catch (Exception e) {
            List<CacheResult> res = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                res.add(CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR));
            }
            return res;
        }
    }

    public List<CacheResult> getByRedisBatch(Serializable... keys) {
        return getByRedisBatch(Arrays.asList(keys));
    }

    @Override
    public CacheResult setSync(Serializable key, Serializable value) {
        if(key==null){
            return CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER);
        }
        try {
            byte[] bKey = serializable(key);
            byte[] bVal = serializable(value);
            CacheEntry entry = new CacheEntry(bVal, CacheEntry.Status.EXIST);
            localSet(key, entry);
            //这里应该给redis也放一份数据，但这里应该用同步还是异步设置这数据？换句话说需要强一致吗？
            //先用同步的形式，但设计得要易于修改成异步的模式
            //异步模式需要考虑线程，用线程池（如果后期有很大的数据一次性put再考虑异步）
            //如果用异步的话就不需要返回了
            redisPut(bKey, bVal);
            return CacheResult.success("OK");
        } catch (Exception e) {
            e.printStackTrace();
            return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
        }
    }

    @Override
    public CacheResult increseSync(Serializable key, long score) {
        if(key==null){
            return CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER);
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Long modifiedValue = jedis.incrBy(serializable(key), score);
            if (modifiedValue == null) {
                return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
            }
            CacheEntry entry = new CacheEntry(serializable(modifiedValue), CacheEntry.Status.EXIST);
            localSet(key, entry);
            CacheResult success = CacheResult.success(modifiedValue,CacheEnum.Source.REDIS);
            return success;
        } catch (IOException e) {
            return CacheResult.fail(CacheEnum.ErrorCode.IO_EXCEPTION);
        } catch (Exception e) {
            return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
        }
    }

    @Override
    public CacheResult existKeySync(Serializable key) {
        if(key==null){
            return CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER);
        }
        try {
            CacheEntry entry = localCache.get(key);
            if (entry != null && !CacheUtils.isExpired(entry, new Date())) {
                CacheResult success = CacheResult.success(Boolean.TRUE,CacheEnum.Source.LOCAL_CACHE);
                return success;
            } else {
                Boolean aBoolean = redisExistsKey(serializable(key));
                if (Boolean.TRUE.equals(aBoolean)) {
                    return CacheResult.success(aBoolean, CacheEnum.Source.REDIS);
                } else {
                    return CacheResult.fail(CacheEnum.ErrorCode.NO_EXIST_REDIS);
                }
            }
        } catch (Exception e) {
            return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
        }
    }

    @Override
    public CacheResult delKeySync(Serializable key) {
        if(key==null){
            return CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER);
        }
        try {
            localRemove(key);
            Long aLong = redisDelKey(serializable(key));
            if (aLong == null) {
                return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
            } else if (aLong == 0) {
                return CacheResult.fail(CacheEnum.ErrorCode.NO_EXIST_REDIS);
            }
            return CacheResult.success(aLong);
        } catch (IOException e) {
            return CacheResult.fail(CacheEnum.ErrorCode.IO_EXCEPTION);
        } catch (Exception e) {
            return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
        }
    }

    /**
     * 我发现一个问题，就是localcache里的expire信息可能会丢失，从而不会因为expire到期而被删除。
     * 也就是说localCahe里的expire不是那么的可靠，建议有被设置expire的数据都从redis里获取
     *
     * @param key
     * @param millisecond
     * @return
     */
    @Override
    public CacheResult expireSync(Serializable key, long millisecond) {
        if(key==null){
            return CacheResult.fail(CacheEnum.ErrorCode.ILLEGAL_PARAMETER);
        }
        try {
            CacheEntry entry = localGet(key);
            if (entry != null) {
                entry.setExpireTime(new Date().getTime() + millisecond);
                localSet(key, entry);
            }
            //这里并不会把redis的数据刷新到本地缓存
            Long aBoolean = redisExpire(serializable(key), millisecond);
            if (aBoolean == 1) {
                return CacheResult.success(aBoolean, CacheEnum.Source.REDIS);
            } else {
                return CacheResult.fail(CacheEnum.ErrorCode.NO_EXIST_REDIS);
            }
        } catch (Exception e) {
            return CacheResult.fail(CacheEnum.ErrorCode.REDIS_ERROR);
        }
    }

    static class SO implements Serializable {
        public static int UID = 100;
        int a = 10;
    }

    //这个性能测试显示localCache用的锁有了点开销
//    get data from localcache cost 230 ms:
//    get data from redis cost 1604 ms:
//    get data from redis using batch way cost 136 ms:
    static void test_localCacheVsRedis() {
        SpeedCacheImpl cache = new SpeedCacheImpl(100000);
        for (int i = 0; i < 100000 / 2; i++) {
            cache.setSync(String.valueOf(i), new SpeedCacheImpl.SO());
        }
        long preTime = System.currentTimeMillis();
        for (int i = 0; i < 100000 / 2; i++) {
            CacheResult cacheResult = cache.get(String.valueOf(i));
        }
        System.out.printf("get data from localcache cost %d ms: \n", System.currentTimeMillis() - preTime);

        preTime = System.currentTimeMillis();
        for (int i = 0; i < 100000 / 2; i++) {
            CacheResult cacheResult = cache.getByRedis(String.valueOf(i));
        }
        System.out.printf("get data from redis cost %d ms: \n", System.currentTimeMillis() - preTime);
        preTime = System.currentTimeMillis();
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100000 / 2; i++) {
            list.add(String.valueOf(i));
        }
        List<CacheResult> batch = cache.getByRedisBatch(list);
        System.out.printf("get data from redis using batch way cost %d ms: \n", System.currentTimeMillis() - preTime);
    }

    //测试类
    public static void main(String[] args) throws IOException {
//        test_localCacheVsRedis();
        byte[] serializable = serializable(null);
        System.out.println(serializable);
        Object deserialize = deserialize(serializable);
        System.out.println(deserialize);
    }
}
