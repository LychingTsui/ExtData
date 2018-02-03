package com.youku.tv.showperson;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonCalculate {
	public static long DateSub(String start, String end) throws ParseException {
		java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyyMMdd");
		java.util.Date beginDate = format.parse(start);
		java.util.Date endDate = format.parse(end);
		long day = (endDate.getTime() - beginDate.getTime()) / (24 * 60 * 60 * 1000);
		return day;
		// System.out.println("相隔的天数="+day);
	}

	public static double WilsonDown(double p, double z, long n) {
		double d = (p + z / (2 * n) - z * Math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)))
				/ (1 + z * z / n);
		return d;
		// DecimalFormat df = new DecimalFormat("0.0000");
		// System.out.println(df.format(d));
	}

	public static double WilsonUp(double p, double z, long n) {
		double d = (p + z / (2 * n) + z * Math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)))
				/ (1 + z * z / n);
		return d;
		// DecimalFormat df = new DecimalFormat("0.0000");
		// System.out.println(df.format(d));
	}

	public static String TimeStamp2Date(long timestampString) {
		Long timestamp = timestampString * 1000;
		String date = new java.text.SimpleDateFormat("yyyyMMdd").format(new java.util.Date(
				timestamp));
		return date;
	}

	public static void main(String[] args) throws ParseException {
		Date date=new Date();
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
		
		System.out.println(DateSub("20161023",dateformat.format(date) ));
		System.out.println(Math.pow(1, 1 / 3.0));
		System.out.println(Math.pow(8, 1 / 2.5));
		System.out.println(Math.pow(3, 1 / 3.0));		
	}
}
