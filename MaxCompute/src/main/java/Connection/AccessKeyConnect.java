package Connection;
/**
 * 这个工程还是不要出现真的账号信息比较好
 */

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;

public class AccessKeyConnect {
    public static void main(String[] args) {
        String accessId = "";
        String accessKey = "";
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        String odpsUrl = "https://service.odps.aliyun.com/api";
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject("aifunai_poi");
        for (Table t : odps.tables()) {
            System.out.println(t.getName());
        }
    }
}
