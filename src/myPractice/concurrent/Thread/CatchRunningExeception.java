package myPractice.concurrent.Thread;


class Handler implements Thread.UncaughtExceptionHandler
{
    @Override
    public void uncaughtException(Thread t, Throwable e)
    {
        System.out.println("error occur");
        e.printStackTrace();
    }
}

class SendExitMessage implements Runnable
{
    @Override
    public void run()
    {
//            while (true) System.out.println("alive");
        System.out.printf("dead",5/0);
    }
}

public class CatchRunningExeception
{

    public static void main(String[] args)
    {
        SendExitMessage s = new SendExitMessage();
        Thread t = new Thread(s);
        t.setUncaughtExceptionHandler(new Handler());
        t.start();
    }

}
