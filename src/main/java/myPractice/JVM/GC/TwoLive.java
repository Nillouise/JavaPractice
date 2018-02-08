package myPractice.JVM.GC;


import java.util.concurrent.ThreadPoolExecutor;
//一次gc，并不会让所有失去引用的对象死亡，因为可能在finalizer方法中，对象会把自己重新植入到别人身上。
//所以gc会先标记这个对象执行了finalize方法没，确实是执行了finalize方法，但它仍没引用时，才会真正gc掉
public class TwoLive
{
    static TwoLive hibernate;
    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        System.out.println("method finalize is running");
        hibernate = this;
    }

    boolean isalive()
    {
        System.out.println("I am alive");
        return true;
    }

    public static void main(String[] args) throws InterruptedException
    {
        hibernate = null;
        TwoLive twoLive = new TwoLive();
        twoLive=null;

        System.gc();
//        感觉这里gc是开了另一个线程，所以才需要隐式等待finalize执行完毕
        Thread.sleep(500);
        if(hibernate==null)
        {
            System.out.println("after finerlizer,it is dead");
        }else{
            hibernate.isalive();
        }

        hibernate = null;
        System.gc();
        Thread.sleep(500);
        if(hibernate==null)
        {
            System.out.println("after finerlizer,gc again,it is dead");
        }else{
            hibernate.isalive();
        }


    }



}
