package myPractice.concurrent.ThreadLocal;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by win7x64 on 2017/9/21.
 */
public class LocalVariable
{
    public static final ThreadLocal<SimpleDateFormat> dateFormat = ThreadLocal.withInitial(()->new SimpleDateFormat("yyyy-MM-dd"));

    public static void main(String[] args)
    {
        SimpleDateFormat dateFormat = LocalVariable.dateFormat.get();
        try
        {
            Date d = dateFormat.parse("2000-3-5");
            System.out.println(d);
        } catch (ParseException e)
        {
            e.printStackTrace();
        }
        String date = dateFormat.format(new Date());
        System.out.println(date);


    }



}
