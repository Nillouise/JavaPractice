package myPractice.concurrent.Thread;
//这程序是失败的，无法测试出，unlock时，到底有没有向condition 发signalall

import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Bank2
{
    final double[] accounts;
    Lock lock;
    Condition condition;

    public Bank2(int accountSize)
    {
        accounts = new double[accountSize];
        Arrays.fill(accounts,100.0);
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    public int transfer(int from,int to,double money)
    {
        lock.lock();
        try
        {
            if(accounts.length != 1)
            {
                System.out.println(Thread.currentThread()+"await");
                condition.await();
            }
            int a=0;
            if(accounts.length == 1)
            {
                a =1/0;
            }
            condition.signalAll();
            System.out.println(Thread.currentThread()+"singal all");
            return a;

        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } finally
        {
            lock.unlock();
        }
        return 0;
    }
}


public class ExceptionWithoutSignalALL
{

    public static void main(String[] args)
    {
        int  banksize = 10;
        Bank2 b1 = new Bank2(1);
        Bank2 b2 = new Bank2(2);

        Thread t = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                b1.transfer(0,0,0);
            }
        });

        Thread t2 = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                b2.transfer(0,0,0);
            }
        });
        t2.start();
        t.start();

    }
}
