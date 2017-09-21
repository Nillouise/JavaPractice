package myPractice.concurrent.BlockingQueue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by win7x64 on 2017/9/21.
 * 用lambda 实现一遍多线程查找文件夹内的不同文件
 */
public class Lambda
{
    static BlockingQueue<File> queue = new LinkedBlockingDeque<>();
    static File dummy = new File("");

    static class searchFile implements Runnable
    {

        @Override
        public void run()
        {
            search();

        }

        void search()
        {
            for(;;)
            {
                try
                {
                    File f = queue.take();
                    if(f==dummy)
                    {
                        return;
                    }else{
                        try(Scanner in = new Scanner(f,"UTF-8"))
                        {
                            String line = in.nextLine();
                            if(line.contains(""));//这里要是用lambda的话，就不需要传这个参数进来了。
                        } catch (FileNotFoundException e)
                        {
                            e.printStackTrace();
                        } ;
                    }

                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args)
    {
        File f = new File("C:\\code");
        Runnable producer = ()->{
            emurate(f);
            try
            {
                queue.put(dummy);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        };

        new Thread(producer).start();





    }

    public static void emurate(File directory)
    {
        File[] fl = directory.listFiles();

        for (File file : fl)
        {
            if(file.isDirectory())
            {
                emurate(file);
            }else{
                try
                {
                    queue.put(file);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }


    }
}
