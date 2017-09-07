package myPractice.concurrent.BlockingQueue;
//利用future计算文件夹内所有文件的总行数

import org.omg.CORBA.INTERNAL;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

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
                FileMatcher fileMatcher = new FileMatcher(f);
                FutureTask<Integer> task = new FutureTask<Integer>(fileMatcher);
                list.add(task);
                new Thread(task).start();
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


public class SearchFile1
{
    public static void main(String[] args)
    {
        FileMatcher fileMatcher = new FileMatcher(new File("C:\\code\\debug"));
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
    }

}
