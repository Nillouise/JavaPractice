package myPractice.Collection;

import java.util.*;

public class Map {

    public static void main(String[] args) {
        HashMap<String, Integer> map = new HashMap<>();
        map.put(null,10);
//        map.put("id1",10);
//        map.put("id2",100);
        for(java.util.Map.Entry<String,Integer> e : map.entrySet())
        {
            System.out.printf("%s,%d\n",e.getKey(),e.getValue());
        }
    }

}
