package myPractice.concurrent.Thread;
//这程序运行一段时间就会deadlock掉
import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Bank
{
    final double[] accounts;
    Lock lock;
    Condition condition;

    public Bank(int accountSize)
    {
        accounts = new double[accountSize];
        Arrays.fill(accounts,100.0);
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    public void transfer(int from,int to,double money)
    {
        lock.lock();
        try
        {
            while (accounts[from]<money)
                condition.await();
            accounts[from]-=money;
            accounts[to]+=money;
            System.out.printf("after transfer %f money,account %d has %f money,account %d has %f money\n",money,from,accounts[from],to,accounts[to]);

            double total =0;
            for (double i : accounts)
            {
                total+=i;
            }
            System.out.println("total:"+total);

            condition.signalAll();

        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } finally
        {
            condition.signalAll();
            lock.unlock();
        }

    }
}

class Transact implements Runnable
{
    Bank bank;
    int banksize;

    public Transact(Bank bank, int banksize)
    {
        this.bank = bank;
        this.banksize = banksize;
    }

    @Override
    public void run()
    {
        while (true)
        {
            int from = (int) (Math.random()*banksize);
            int to = (int) (Math.random()*banksize);
            bank.transfer(from,to,Math.random()*100);
        }
    }
}

public class BandCondition
{

    public static void main(String[] args)
    {
        int  banksize = 10;
        Bank b = new Bank(banksize);

        for (int i=0;i<10;i++)
        {
            Transact act = new Transact(b,banksize);
            Thread t = new Thread(act);
            t.start();
        }
    }
}
