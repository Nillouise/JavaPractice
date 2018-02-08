package myPractice.concurrent.synchronize;//1．编写多线程应用程序，模拟多个人通过一个山洞的模拟。这个山洞每次只能通过一个人，每个人通过山洞的时间为5秒，随机生成10个人，同时准备过此山洞，显示一下每次通过山洞人的姓名。

class Cave
{

}


class Person implements Runnable
{
    public String name;
    public Cave cave;
    public Person(Cave cave)
    {
        name = String.valueOf(Math.random());
        this.cave = cave;
    }
    public void through() throws InterruptedException
    {
        synchronized (cave)
        {
            Thread.sleep(2000);
        }
        System.out.println(toString());
    }

    @Override
    public String toString()
    {
        return "Person{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public void run()
    {
        try
        {
            through();
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }
}

public class MimicCave
{
    public static void main(String[] args)
    {
        Cave cave = new Cave();
        for (int i = 0; i < 10; i++)
        {
            Thread t = new Thread(new Person(cave));
            t.start();
        }

    }
}
