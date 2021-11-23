package com.ljn.ss.util;

import java.util.ArrayList;
import java.util.List;

public class RankUtils {
    private static  List<Integer> rankList;

    public static void initRankUtil(List<Integer> lst){
        if(lst!=null){
            rankList = lst;
        }else{
            rankList = new ArrayList<>();
            rankList.add(0);
            rankList.add(10*60);
            rankList.add(20*60);
            rankList.add(30*60);
            rankList.add(40*60);
        }
    }


    public  static int getRank(int v){
        int n = rankList.size();
        for(int i = 1;i<n;i++)
        {
            if(rankList.get(i-1)<=v && rankList.get(i)>=v)
                return i-1;
        }
        return n;
    }
}
