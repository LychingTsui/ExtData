package com.youku.tv.movieperson.reclist20160420;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.TreeMultimap;

public class Utils {
	public static String sortRecList(String recList, int cutoff) {
		TreeMultimap<Double, String> recResultMultimap = TreeMultimap.create(
				Collections.reverseOrder(), Collections.reverseOrder());
		String[] cfResults = recList.split("\t");
		for (String cfResult : cfResults) {
			String[] members = cfResult.split("#@#");
			if (members.length >= 4) {
				recResultMultimap.put(Double.parseDouble(members[3]), cfResult);
			}
		}

		Set<Double> keyDoubles = recResultMultimap.keySet();
		Iterator<Double> iterator = keyDoubles.iterator();

		int count = 0;
		StringBuffer sbSortedRecList = new StringBuffer();
		while (iterator.hasNext()) {
			Double confidence = iterator.next();
			Collection<String> collection = recResultMultimap.get(confidence);
			for (String c : collection) {
				sbSortedRecList.append("\t");
				sbSortedRecList.append(c);
				count++;
				if (count == cutoff) {
					recResultMultimap.clear();
					return sbSortedRecList.substring(1);
				}
			}
		}
		recResultMultimap.clear();
		if (sbSortedRecList.length() == 0) {
			return "";
		}
		return sbSortedRecList.substring(1);
	}
	//判断一个字符串是否是整形
	public static boolean isInteger(String value) {
		try {
			Double.parseDouble(value);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	public static String sortRecList(String recList, int cutoff, int index, String separteA,
			String separteB) {
		//对map集合长的key和value进行倒叙排序 美国：0.7，英国：0.8，法国：0.6
		TreeMultimap<Double, String> recResultMultimap = TreeMultimap.create(
				Collections.reverseOrder(), Collections.reverseOrder());
		 //对字符串进行分割，就是得到多条电影播放或点击信息
		String[] cfResults = recList.split(separteA);
		for (String cfResult : cfResults) {
			//对每一个电影进行细分，得到该电影的播放时长，播放次数，播放日期，
			String[] members = cfResult.split(separteB);
				if (isInteger(members[index])) {
					//存储的数据格式：key 20160517,value movieid+播放时长＋次数＋时间
					//样例：0.7 美国：0.7
					recResultMultimap.put(Double.parseDouble(members[index]), cfResult);
				}	
		}

		Set<Double> keyDoubles = recResultMultimap.keySet();
		Iterator<Double> iterator = keyDoubles.iterator();

		int count = 0;
		StringBuffer sbSortedRecList = new StringBuffer();
		while (iterator.hasNext()) {
			Double confidence = iterator.next();
			//按照字符串的格式进行整理
			Collection<String> collection = recResultMultimap.get(confidence);
			for (String c : collection) {
				sbSortedRecList.append(separteA);
				sbSortedRecList.append(c);
				count++;
				//判断截取位
				if (count == cutoff) {
					recResultMultimap.clear();
					return sbSortedRecList.substring(separteA.length());
				}
			}
		}
		recResultMultimap.clear();
		if (sbSortedRecList.length() == 0) {
			return "";
		}
		return sbSortedRecList.substring(separteA.length());
	}

	public static ArrayList<String> sortRecList(ArrayList<String> recList, int cutoff) {
		ArrayList<String> linkList = new ArrayList<String>();
		TreeMultimap<Double, String> recResultMultimap = TreeMultimap.create(
				Collections.reverseOrder(), Collections.reverseOrder());
		for (String cfResult : recList) {
			String[] members = cfResult.split("#@#");
			if (members.length >= 4) {
				recResultMultimap.put(Double.parseDouble(members[3]), cfResult);
			}
		}
		Set<Double> keyDoubles = recResultMultimap.keySet();
		Iterator<Double> iterator = keyDoubles.iterator();
		int count = 0;
		while (iterator.hasNext()) {
			Double confidence = iterator.next();
			Collection<String> collection = recResultMultimap.get(confidence);
			for (String c : collection) {
				linkList.add(c);
				count++;
				if (count == cutoff) {
					return linkList;
				}
			}
		}
		return linkList;

	}

	public static String Normalize(String info, String seprateA, String seprateB, int index,
			double maxvalue, double minvalue) {
		DecimalFormat df = new DecimalFormat("0.0000");
		StringBuffer sb = new StringBuffer();
		String[] data = info.split(seprateA);
		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;
		for (int i = 0; i < data.length; i++) {
			String[] temp = data[i].split(seprateB);
			double d = Double.valueOf(temp[index]);
			if (d > max) {
				max = d;
			}
			if (d < min) {
				min = d;
			}
		}

		for (int i = 0; i < data.length; i++) {
			String[] temp = data[i].split(seprateB);
			if (max == min) {
				temp[index] = df.format(maxvalue);
			} else {
				double d = Double.valueOf(temp[index]);
				double t = (d - min) * (maxvalue - minvalue) / (max - min) + minvalue;
				temp[index] = df.format(t);
			}

			StringBuffer s = new StringBuffer();
			for (int j = 0; j < temp.length; j++) {
				s.append(seprateB).append(temp[j]);
			}
			data[i] = s.substring(1);
			sb.append(seprateA).append(data[i]);
		}
		return sb.substring(1);
	}

	public static String Normalize(String info, String seprateA, String seprateB, int index,
			double maxvalue, double minvalue, double oldmaxvalue, double oldminvalue) {
		DecimalFormat df = new DecimalFormat("0.0000");
		StringBuffer sb = new StringBuffer();
		String[] data = info.split(seprateA);

		for (int i = 0; i < data.length; i++) {
			String[] temp = data[i].split(seprateB);
			if (oldmaxvalue == oldminvalue) {
				temp[index] = df.format(maxvalue);
			} else {
				double d = Double.valueOf(temp[index]);
				double t = (d - oldminvalue) * (maxvalue - minvalue) / (oldmaxvalue - oldminvalue)
						+ minvalue;
				temp[index] = df.format(t);
			}

			StringBuffer s = new StringBuffer();
			for (int j = 0; j < temp.length; j++) {
				s.append(seprateB).append(temp[j]);
			}
			data[i] = s.substring(1);
			sb.append(seprateA).append(data[i]);
		}
		return sb.substring(1);
	}

	public static long DateSub(String start, String end) throws ParseException {
		java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyyMMdd");
		java.util.Date beginDate = format.parse(start);
		java.util.Date endDate = format.parse(end);
		long day = (endDate.getTime() - beginDate.getTime()) / (24 * 60 * 60 * 1000);
		return day;
		// System.out.println("相隔的天数="+day);
	}

	public static long DateSub(String start) throws ParseException {
		java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyyMMdd");
		java.util.Date beginDate = format.parse(start);
		java.util.Date endDate = format.parse(format.format(new Date()));
		long day = (endDate.getTime() - beginDate.getTime()) / (24 * 60 * 60 * 1000);
		return day;
	}

	public static long DateSubYear(String start) throws ParseException {
		java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy");
		java.util.Date beginDate = format.parse(start);
		java.util.Date endDate = format.parse(format.format(new Date()));
		long day = (endDate.getTime() - beginDate.getTime()) / (24 * 60 * 60 * 1000)/365;
		return day;
	}

	public static void main(String[] args) throws ParseException {
		String start="1987";
		//System.out.println(DateSubYear(start));
		//System.out.println(Math.log(8+ Utils.DateSubYear(start)));
		//System.out.println(DateSub(start));
		String sbd="\2成龙:0.95,\2甑子丹:0.7,\2小花:0.8,中国:0.3,England:0.9,America:0.5";
		String bb=sortRecList(sbd, 0, 1, ",", ":");
		System.out.println(bb);
		System.out.println(DateSubYear("2016"));
//		String str = "tduploader88175950:0.0130,tduploader67361417:0.0122,tduploader89523896:0.0115";
//		//System.out.println(Normalize(str, ",", ":", 1, 0.9, 0.1));
	}
}
