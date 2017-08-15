package com;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class M {

    public static void main(String[] args){
        String str = "[WARN ] [com.mchange.v2.resourcepool.BasicResourcePool] [2017-08-07 06:46:36,141]";
        String str2="2017-08-07 10:25:02,384 [// -  - ] DEBUG com.cmrh.sf.rpc.skeleton.JavaHttpRpcSkeletonProviderPlugin";
        String time ="2017-07-26T02:21:14 UTC";
        String time1 = "2017-07-26T02:21:14.741135385Z";
        System.out.println(time1.substring(0,19));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss z");
        try {
            System.out.println(sdf.parse(time));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})");
        Matcher matcher = pattern.matcher(time1);
        if(matcher.find()){
            System.out.println(matcher.group(1));
        }


    }
}
