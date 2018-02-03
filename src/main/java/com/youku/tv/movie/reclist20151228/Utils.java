package com.youku.tv.movie.reclist20151228;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

	public static String sortRecList(String recList, int cutoff, int index, String separteA,
			String separteB) {
		TreeMultimap<Double, String> recResultMultimap = TreeMultimap.create(
				Collections.reverseOrder(), Collections.reverseOrder());
		String[] cfResults = recList.split(separteA);
		for (String cfResult : cfResults) {
			// String[] members = cfResult.split(separteB);
			int idex = cfResult.lastIndexOf(separteB);
			if (idex > -1) {
				recResultMultimap.put(Double.parseDouble(cfResult.substring(idex + 1)), cfResult);
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
				sbSortedRecList.append(separteA);
				sbSortedRecList.append(c);
				count++;
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

	public static String sortRecListBB(String recList, int cutoff, int index, String separteA,
			String separteB) {
		TreeMultimap<Double, String> recResultMultimap = TreeMultimap.create(
				Collections.reverseOrder(), Collections.reverseOrder());
		String[] cfResults = recList.split(separteA);
		for (String cfResult : cfResults) {
			String[] members = cfResult.split(separteB);
			recResultMultimap.put(Double.parseDouble(members[index]), cfResult);
		}

		Set<Double> keyDoubles = recResultMultimap.keySet();
		Iterator<Double> iterator = keyDoubles.iterator();

		int count = 0;
		StringBuffer sbSortedRecList = new StringBuffer();
		while (iterator.hasNext()) {
			Double confidence = iterator.next();
			Collection<String> collection = recResultMultimap.get(confidence);
			for (String c : collection) {
				sbSortedRecList.append(separteA);
				sbSortedRecList.append(c);
				count++;
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

	public static String sortRecListN(String recList, int cutoff, int index, String separteA,
			String separteB) {
		TreeMultimap<Double, String> recResultMultimap = TreeMultimap.create(
				Collections.reverseOrder(), Collections.reverseOrder());
		String[] cfResults = recList.split(separteA);
		for (String cfResult : cfResults) {
			String[] members = cfResult.split(separteB);
			recResultMultimap.put(Double.parseDouble(members[index]), cfResult);
		}

		Set<Double> keyDoubles = recResultMultimap.keySet();
		Iterator<Double> iterator = keyDoubles.iterator();

		int count = 0;
		StringBuffer sbSortedRecList = new StringBuffer();
		while (iterator.hasNext()) {
			Double confidence = iterator.next();
			Collection<String> collection = recResultMultimap.get(confidence);
			for (String c : collection) {
				sbSortedRecList.append(separteA);
				sbSortedRecList.append(c);
				count++;
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

	public static void main(String[] args) {
		String str = "tduploader88175950:0.0130,tduploader67361417:0.0122,tduploader89523896:0.0115";
		System.out.println(sortRecListBB(str,10,1, ",", ":"));
		//System.out.println(Normalize(str, ",", ":", 1, 0.9, 0.1));
	}
}
