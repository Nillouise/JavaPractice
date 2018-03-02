package redis.schedulerX;

import com.alibaba.edas.schedulerx.SchedulerXClient;

public class schedulerxTestMain {
    public static void main(String[] args) {
        SchedulerXClient schedulerXClient = new SchedulerXClient();
        schedulerXClient.setGroupId("101-1-2-4236");
        schedulerXClient.setRegionName("cn-hangzhou");
     /*
     //如果使用的是上海 Region 集群，需要设置 domainName 属性，同时指定 RegionName 为 cn-shanghai
     schedulerXClient.setRegionName("cn-shanghai");
     schedulerXClient.setDomainName("schedulerx-shanghai.console.aliyun.com");
     */
        try {
            schedulerXClient.init();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}