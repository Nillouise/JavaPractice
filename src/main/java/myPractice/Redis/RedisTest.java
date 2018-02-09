package myPractice.Redis;

import org.jredis.JRedis;
import org.jredis.connector.ConnectionSpec;
import org.jredis.ri.alphazero.JRedisClient;
import org.jredis.ri.alphazero.JRedisPipelineService;
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.*;

/**
 * 需要在服务器端启动redis才行
 */
public class RedisTest {
    //采用pipeline 方式发送指令
    private static void usePipeline() {
        try {
            ConnectionSpec spec = DefaultConnectionSpec.newSpec(
                    "localhost", 7000, 0, null);
            JRedis jredis = new JRedisPipelineService(spec);
            for (int i = 0; i < 100000; i++) {
                jredis.incr("test2");
            }
            jredis.quit();
        } catch (Exception e) {
        }
    }
    //普通方式发送指令
    private static void withoutPipeline() {
        try {
            JRedis jredis = new JRedisClient("localhost",7000);
            System.out.println(jredis.ping());
            for (int i = 0; i < 100000; i++) {
                jredis.incr("test2");
            }
            jredis.quit();
        } catch (Exception e) {
        }
    }
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
//采用pipeline 方式发送指令
        usePipeline();
        long end = System.currentTimeMillis();
        System.out.println("用pipeline 方式耗时：" + (end - start) + "毫秒");
        start = System.currentTimeMillis();
//普通方式发送指令
        withoutPipeline();
        end = System.currentTimeMillis();
        System.out.println("普通方式耗时：" + (end - start) + "毫秒");

    }
}
