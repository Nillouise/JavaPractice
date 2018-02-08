package myPractice.concurrent.Thread;

/**
 * Created by win7x64 on 2017/9/19.
 */
public class Daemon
{
    static class SendExitMessage implements Runnable
    {
        @Override
        public void run()
        {
//            while (true) System.out.println("alive");
            System.out.println("dead");
        }
    }

    public static void main(String[] args) throws InterruptedException
    {
        SendExitMessage sendExitMessage = new SendExitMessage();
        Thread t = new Thread(sendExitMessage);
        t.setDaemon(true);
        t.start();
//        Thread.sleep(100);
        System.exit(0);
    }

}
