package com.lrz.storm.util;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	/**
	 * getMinute:获取指定时间的正点分钟时间戳. <br/>
	 * 
	 * @param time
	 * @return
	 * @throws ParseException
	 */
	public static long getMinute(long time) throws ParseException {
		Date date = new Date(time);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		date = calendar.getTime();
		return date.getTime();
	}
}
