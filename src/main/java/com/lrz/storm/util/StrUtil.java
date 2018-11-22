package com.lrz.storm.util;

import com.alibaba.fastjson.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by sunqifs on 12/10/17.
 */

public class StrUtil {

    public static boolean isEmpty(Object str) {
        if(str == null)
            return true;
        String tempStr = str.toString().trim();
        if(tempStr.length() == 0)
            return true;
        if(tempStr.equals("null"))
            return true;
        return false;
    }

    public static String toStr(Object obj) {
        if (obj == null)
            return "";
        else return obj.toString();
    }

    public static String date2String(String pattern, Date date) {
        String retStr = "";
        java.text.SimpleDateFormat ff = new java.text.SimpleDateFormat();
        ff.applyPattern(pattern);
        retStr = ff.format(date);
        return retStr;
    }

    public static String date2String(String pattern, Date date, String timezone) {
        String retStr = "";
        java.text.SimpleDateFormat ff = new java.text.SimpleDateFormat();
        ff.setTimeZone(TimeZone.getTimeZone(timezone));
        ff.applyPattern(pattern);
        retStr = ff.format(date);
        return retStr;
    }

    public static Date string2Date(String pattern, String str) {
        Date date = new Date();
        if (isEmpty(str)) {
            return date;
        }
        java.text.SimpleDateFormat ff = new java.text.SimpleDateFormat();
        ff.applyPattern(pattern);
        try {
            date = ff.parse(str);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            //logger.error(e.getMessage(), e);
        }
        return date;
    }

    public static String toKeyValueStringGeneral(Map<String, String> param) {
        if (StrUtil.isEmpty(param) || param.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        for (String key : param.keySet()) {
            String value = param.get(key);
            try {
                sb.append(key + "=" + URLEncoder.encode(value, "UTF-8") + "&");
            } catch (UnsupportedEncodingException e) {
                //logger.error(e.getMessage());
                e.printStackTrace();
            }
        }
        return sb.toString().substring(0, sb.length() - 1);
    }

    public static double getAvgPriceFromOrderInfo(String order) throws NullPointerException {
        JSONObject orderObject = JSONObject.parseObject(order);
        JSONObject innerObject = orderObject.getJSONArray("orders").getJSONObject(0);
        return innerObject.getDouble("avg_price");
    }

    public static double getDealAmountFromOrderInfo(String order) throws NullPointerException {
        JSONObject orderObject = JSONObject.parseObject(order);
        JSONObject innerObject = orderObject.getJSONArray("orders").getJSONObject(0);
        return innerObject.getDouble("deal_amount");
    }

    public static int getStatusFromOrderInfo(String order) throws NullPointerException {
        JSONObject orderObject = JSONObject.parseObject(order);
        JSONObject innerObject = orderObject.getJSONArray("orders").getJSONObject(0);
        return innerObject.getIntValue("status");
    }
}
