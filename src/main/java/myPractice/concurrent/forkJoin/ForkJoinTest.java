package myPractice.concurrent.forkJoin;

//RecursiveTask、ForkJoinPool就是利用多个线程解决分治法的框架而已。

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

class HalveDivide extends RecursiveTask<Integer>
{
    private double[] array;
    private int from;
    private int to;
    private Filter filter;

    public HalveDivide(double[] array, int from, int to, Filter filter)
    {
        this.array = array;
        this.from = from;
        this.to = to;
        this.filter = filter;
    }

    @Override
    protected Integer compute()
    {
        if(to-from<1000)
        {
            int cnt =0;
            for (int i = from; i < to; i++)
            {
                if(filter.accept(array[i]))cnt++;
            }
            return cnt;
        }

        int mid = (from+to)/2;
        HalveDivide left = new HalveDivide(array,from,mid,filter);
        HalveDivide right = new HalveDivide(array,mid,to,filter);
        invokeAll(left,right);

        return left.join()+right.join();
    }
}

interface Filter
{
    boolean accept(double x);
}

public class ForkJoinTest
{
    public static void main(String[] args)
    {
        double array[] = new double[100000000];
        for(int i=0;i<array.length;i++)array[i]=Math.random();

        HalveDivide halveDivide = new HalveDivide(array, 0, array.length,
                x -> x>0.5);
//        这里加了个pool，应该在pool invoke这里把RecursiveTask设置成自己的了，然后在HalveDivide的invokeall就会自动调用这个设置的pool了
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(halveDivide);
        System.out.println(halveDivide.join());

    }
    
}
