package redis;

import java.util.Date;

/**
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-23
 **/
public class CacheUtils {
    public static boolean isExpired(CacheEntry e, Date d)
    {
        return e.getExpireTime() != null && e.getExpireTime() < d.getTime();
    }
}
