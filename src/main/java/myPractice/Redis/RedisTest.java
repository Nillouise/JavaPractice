package myPractice.Redis;

import redis.clients.jedis.Jedis;

/**
 * 需要在服务器端启动redis才行
 */
public class RedisTest {
    public static void main(String[] args) {
        //连接本地的 Redis 服务
        Jedis jedis = new Jedis("localhost");
        System.out.println("连接成功");
        //查看服务是否运行
        System.out.println("服务正在运行: "+jedis.ping());
    }
}
