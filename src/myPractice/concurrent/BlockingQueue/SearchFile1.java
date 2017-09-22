package myPractice.concurrent.BlockingQueue;
//利用future计算文件夹内所有文件的总行数

import org.omg.CORBA.INTERNAL;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

class FileMatcher implements Callable<Integer>
{
    File baseDirectory;

    public FileMatcher(File baseDirectory)
    {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public Integer call() throws Exception
    {
        File[] files = baseDirectory.listFiles();
        int cnt = 0;
        List<FutureTask<Integer>> list = new ArrayList<>();
        for (File f:files)
        {
            if(f.isDirectory())
            {
                multithreadProcess(f,list);
            }else{
                cnt+=search(f);
            }
        }
        for (FutureTask<Integer> task : list)
        {
            cnt += task.get();
        }
        return cnt;
    }

    //这里把线程实现用的技术独立出来，用什么线程技术处理一个directory，方便子类更改
    public int multithreadProcess(File directory,List<FutureTask<Integer>> list)
    {
        FileMatcher fileMatcher = new FileMatcher(directory);
        FutureTask<Integer> task = new FutureTask<Integer>(fileMatcher);
        list.add(task);
        new Thread(task).start();
        return 0;
    }

    public Integer search(File f)
    {
        int cnt=0;
        try
        {
            Scanner scanner = new Scanner(f);
            while (scanner.hasNextLine())
            {
                cnt++;
                scanner.nextLine();
            }
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }finally
        {
            return cnt;
        }
    }

}

//用一个pool而不是看见一个新文件夹就创建新一条线程
class ExecutorFileMatch extends FileMatcher
{
    ExecutorService pool;
    List<FutureTask<Integer>> list;

    public ExecutorFileMatch(File baseDirectory, ExecutorService pool,List<FutureTask<Integer>> list )
    {
        super(baseDirectory);
        this.pool = pool;
        this.list = list;
    }

    @Override
    public int multithreadProcess(File directory, List<FutureTask<Integer>> noNeed)
    {
        ExecutorFileMatch fileMatcher = new ExecutorFileMatch(directory,pool,this.list);
        FutureTask<Integer> task = new FutureTask<Integer>(fileMatcher);
        pool.submit(task);//感觉是pool不支持多线程插入任务
        list.add(task);
        return 0;
    }

}


public class SearchFile1
{
    public static void main(String[] args)
    {
        FileMatcher fileMatcher = new FileMatcher(new File("C:\\code"));
        FutureTask<Integer> task = new FutureTask<Integer>(fileMatcher);
        new Thread(task).start();
        try
        {
            System.out.println(task.get());
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } catch (ExecutionException e)
        {
            e.printStackTrace();
        }


        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<FutureTask<Integer>> list = new ArrayList<>();

        FileMatcher excuMatcher = new ExecutorFileMatch(new File("C:\\code"),pool,list);

//        ExecutorFileMatch excuMatcher = new ExecutorFileMatch(new File("C:\\code\\debug"),pool,list);
        FutureTask<Integer> et = new FutureTask<Integer>(excuMatcher);
        list.add(et);
        pool.submit(et);

        int total = 0;
//        for (FutureTask<Integer> futureTask : list)//出现ConcurrentModificationException，因为子线程改了这个list
        pool.shutdown();//shutdown之后，当所有线程都执行完毕，就会产生terminate信号
        while (!pool.isTerminated());
        for (FutureTask<Integer> futureTask : list)
        {
            try
            {
                total += futureTask.get();
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            } catch (ExecutionException e)
            {
                e.printStackTrace();
            }
        }
        System.out.println(total);

    }

}
