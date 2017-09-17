package myPractice.RMI;
//现有如下信息：
//用户列表数据 :List<Long>uidList, size=10000
//消费查询远程接口： long  queryByUid(long uid)
//请根据用户信息及远程接口，写一段程序统计用户的平均消费。
//要求：以性能最优实现

//总的来说，我觉得要熟悉一下多线程各个类的关系才行，起码要认得长什么样子。
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class MulGetRemote
{
    Long initAndRun()
    {
        List<Integer> list = new ArrayList<>();
        list.add(1);

        return mul(list);
    }

    Long mul(List<Integer> uids)
    {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        List<FutureTask<Long>> results = new ArrayList<>();
        for(Integer i:uids)
        {
            FutureTask<Long> future = new FutureTask<Long>(new RequestRemote(i));
            executor.execute(future);
            results.add(future);
        }

        int total = 0;

        try{

            for (FutureTask<Long> result : results)
            {
                total+=result.get();
            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } catch (ExecutionException e)
        {
            e.printStackTrace();
        }

        executor.shutdown();//这句忘记了写了。

        return (long)total;

    }

    class RequestRemote implements Callable<Long>
    {
        int uid;
        public RequestRemote(int uid)
        {
            this.uid = uid;
        }

        @Override
        public Long call() throws Exception
        {
            return requestConsume();
        }

        public Long requestConsume()
        {
            //这里利用uid查询远程数据库
            Long result= 100L;
            return result;
        }

    }

    public static void main(String[] args)
    {

    }
}
