package redis;

import java.io.*;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import redis.clients.jedis.Jedis;

import javax.xml.transform.Result;

/**
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public class MyCacheImpl implements MyCache {

    private LRULocalCache<Object, CacheEntry> localCache;
    Jedis jedis;
    private int localCacheSize;
    private String remoteServerUrl;

    public MyCacheImpl(int localCacheSize) {
        this.localCacheSize = localCacheSize;
        this.localCache = new LRULocalCache<>(16, 0.75f, true, localCacheSize);
        remoteServerUrl = "localhost";
        jedis = new Jedis(remoteServerUrl,8000);
    }

    //目前还没查清jedis会怎么抛出异常
    private byte[] redisGet(byte[] key)
    {
        return jedis.get(key);
    }

    private String redisPut(byte[] key,byte[] val)
    {
        String set = jedis.set(key, val);
        return set;
    }

    @Override
    public ResultDTO get(Object key) {
        try {
            //在本地缓存找
            CacheEntry cacheEntry = localCache.get(key);
            if (cacheEntry != null && CacheEntry.Status.EXIST.equals(cacheEntry.getStatus())) {
                ResultDTO resultDTO = ResultDTO.success(deserialize(cacheEntry.getData()));
                resultDTO.setSource("localcache");
                return resultDTO;
            }else{
                //这里开始上redis找缓存
                byte[] bKey = serializable(key);
                byte[] response = redisGet(bKey);
                ResultDTO success = ResultDTO.success(deserialize(response));
                success.setSource("redis");
                return success;
            }
        } catch (Exception e) {
            return ResultDTO.fail(MyConstant.FAIL);
        }
    }

    private byte[] serializable(Object value) throws IOException {
        // 序列化
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        HessianOutput ho = new HessianOutput(os);
        ho.writeObject(value);
        return os.toByteArray();
    }

    private Object deserialize(byte[] by) throws IOException {
        if (by == null) throw new NullPointerException();
        ByteArrayInputStream is = new ByteArrayInputStream(by);
        HessianInput hi = new HessianInput(is);
        return hi.readObject();
    }


    @Override
    public ResultDTO put(Object key, Object value) {
        try {
            byte[] bKey = serializable(key);
            byte[] bVal = serializable(value);
            CacheEntry entry = new CacheEntry(bVal, CacheEntry.Status.EXIST);
            localCache.put(key, entry);
            //这里应该给redis也放一份数据，但这里应该用同步还是异步设置这数据？换句话说需要强一致吗？
            redisPut(bKey,bVal);
            return ResultDTO.success("OK");
        } catch (Exception e) {
            e.printStackTrace();
            return ResultDTO.fail(MyConstant.FAIL);
        }
    }

    //测试类
    public static void main(String[] args) {
        MyCache cache = new MyCacheImpl(6);
        for (int i = 0; i < 10; i++) {
            cache.put(String.valueOf(i), String.valueOf(100 + i));
        }

        ResultDTO resultDTO = cache.get(String.valueOf(1));
        System.out.println(resultDTO.getData());
    }
}
