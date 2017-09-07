package myPractice.concurrent.BlockingQueue;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

class Filematcher implements Callable<Integer>
{

    private File directory;
    private String keyword;
    private ExecutorService pool;

    public Filematcher(File directory, String keyword, ExecutorService pool)
    {
        this.directory = directory;
        this.keyword = keyword;
        this.pool = pool;
    }

    public int dfs(File file)
    {
        File[] files = file.listFiles();
        int cnt=0;
        List<Future<Integer>> list = new ArrayList<>();
        for (File f : files)
        {
            if(f.isDirectory())
            {
//                Filematcher fileMatcher = new Filematcher(file,keyword,pool);
                //这里创建子任务时，用的是父任务的文件夹，导致无限递归，一开始我还以为是
                //pool的submit有问题，都没有考虑递归程序可能导致的死循环。
                Filematcher fileMatcher = new Filematcher(f,keyword,pool);

                list.add(pool.submit(fileMatcher));
            } else{
                cnt+=search(f);
            }
        }
        for (Future<Integer> future : list)
        {
            try
            {
                cnt+=future.get();
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            } catch (ExecutionException e)
            {
                e.printStackTrace();
            }
        }
        return cnt;
    }

    @Override
    public Integer call() throws Exception
    {
        return dfs(directory);
    }

    int search(File file)
    {
        int cnt = 0;
        try
        {
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine())
            {
                String s = scanner.nextLine();
                if (s.contains(keyword))
                {
                    cnt++;
                }
            }

        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        } finally
        {
            return cnt;
        }
    }

}

public class SearchFile2
{
    public static void main(String[] args)
    {
        ExecutorService pool = Executors.newCachedThreadPool();
        Filematcher fileMatcher = new Filematcher(new File("C:\\code"),"Nillouise",pool);
        Future<Integer> result = pool.submit(fileMatcher);
        try
        {
            System.out.println(result.get()+" matching file");
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } catch (ExecutionException e)
        {
            e.printStackTrace();
        }
        pool.shutdown();
        int largestPoolSize = ((ThreadPoolExecutor)pool).getLargestPoolSize();
        System.out.println("largest pool size="+largestPoolSize);
    }
}
