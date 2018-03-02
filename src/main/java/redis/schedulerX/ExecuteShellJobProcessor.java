package redis.schedulerX;

import java.util.Date;
import com.alibaba.edas.schedulerx.ProcessResult;
import com.alibaba.edas.schedulerx.ScxSimpleJobContext;
import com.alibaba.edas.schedulerx.ScxSimpleJobProcessor;
public class ExecuteShellJobProcessor implements ScxSimpleJobProcessor {
    public ProcessResult process(ScxSimpleJobContext context) {
        System.out.println("Hello World! "+new Date());
        return new ProcessResult(true);//true 表示执行成功，false 表示失败
    }
}
