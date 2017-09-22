package myPractice.concurrent.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

/**
 * Created by win7x64 on 2017/9/21.
 */
public class CMAP
{

    public static void main(String[] args)
    {
        ConcurrentHashMap<String,Long> map = new ConcurrentHashMap<>();
        map.merge("fdsf",1L,(x,y)->x+y);
        map.merge("fdsf",1L,(x,y)->x+y);
        System.out.println(map.get("fdsf"));

        FutureTask<Integer> futureTask;
    }
}
