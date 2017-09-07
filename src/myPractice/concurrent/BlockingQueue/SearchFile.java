package myPractice.concurrent.BlockingQueue;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

//多线程搜索文件夹内的关键字，速度好像比单线程快一倍，但无论开多少线程，都也只能快一倍左右
class FileProducer implements Runnable
{
    String initdirectory;
    BlockingQueue<File> queue;
    File dummy;

    public FileProducer(String initdirectory, BlockingQueue<File> queue,File dummy)
    {
        this.initdirectory = initdirectory;
        this.queue = queue;
        this.dummy = dummy;
    }

    public void produce(File curDirecutory)
    {
        File[] files = curDirecutory.listFiles();
        try
        {
            for (File file : files)
            {
                if (file.isDirectory())
                {
                    produce(file);
                } else
                {
                    queue.put(file);
                }
            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        File file = new File(initdirectory);
        produce(file);
        try
        {
            queue.put(dummy);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }
}

class FileConsumer implements Runnable
{
    BlockingQueue<File> queue;
    File dummy;
    String keyword;
    public FileConsumer(BlockingQueue<File> queue, File dummy, String keyword)
    {
        this.queue = queue;
        this.dummy = dummy;
        this.keyword = keyword;
    }

    void search()
    {
        try
        {
            while(true)
            {

                File file = queue.take();
                if(file == dummy)
                {
                    queue.put(file);
                    return;
                }else{
                    Scanner scanner = new Scanner(file);
                    String s;
                    int linecnt=0;
                    while(scanner.hasNextLine())
                    {
                        linecnt++;
                        s = scanner.nextLine();
                        if(s.contains(keyword))
                        {
                            System.out.printf("%s %d has keyword: %s",file.getName(),linecnt,keyword);
                        }
                    }
                }
            }

        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        search();
        System.out.println("thread compelete");
    }
}


public class SearchFile
{
    public static void main(String[] args)
    {
        long startTime = System.currentTimeMillis();
        File dummy = new File("");
        BlockingQueue<File> queue = new ArrayBlockingQueue<File>(50);
        Thread fileProducer = new Thread (new FileProducer("C:\\code",queue,dummy));
        fileProducer.start();
        int threadcnt = 3;
        Thread t[] = new Thread[threadcnt];
        for (int i = 0; i < threadcnt; i++)
        {
            t[i] = new Thread(new FileConsumer(queue,dummy,"Nillouise"));
            t[i].start();
        }
        //利用join 来收到所有线程结束的信号。
        for (int i = 0; i < threadcnt; i++)
        {
            try
            {
                t[i].join();
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }

}
