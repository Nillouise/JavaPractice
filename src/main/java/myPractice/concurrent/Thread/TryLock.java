package myPractice.concurrent.Thread;

import com.sun.scenario.effect.impl.prism.sw.PSWDrawable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by win7x64 on 2017/9/21.
 */

public class TryLock
{
    public static void main(String[] args)
    {
        Lock lock = new ReentrantLock();
        try
        {
            lock.tryLock(100, TimeUnit.DAYS);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }
}
