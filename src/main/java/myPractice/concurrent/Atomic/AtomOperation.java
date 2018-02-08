package myPractice.concurrent.Atomic;


import java.util.concurrent.atomic.AtomicLong;

public class AtomOperation
{
    public static void main(String[] args)
    {
        AtomicLong l = new AtomicLong(100);
        l.updateAndGet(x->Math.max(x,10));
        l.get();

    }
}
