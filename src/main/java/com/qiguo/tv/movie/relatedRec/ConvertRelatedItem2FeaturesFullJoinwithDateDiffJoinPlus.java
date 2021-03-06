package com.qiguo.tv.movie.relatedRec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;

import com.qiguo.tv.movie.featCollection.Tup2;
import com.qiguo.tv.movie.featuresCollection.Pair2;

public class ConvertRelatedItem2FeaturesFullJoinwithDateDiffJoinPlus {
	HashMap<String, String>movieItemsMp = new HashMap<String, String>();
	public ConvertRelatedItem2FeaturesFullJoinwithDateDiffJoinPlus(String path)
			throws IOException{
		this.movieItemsMp = getmovieItemsMp(path);
	}
	
	public HashMap<String, String> getmovieItemsMp(String path)
			throws IOException{
		HashMap<String, String> moviesMap = new HashMap<String, String>();
		BufferedReader bfr = new BufferedReader(new FileReader(path));
		String line = "";
		while((line = bfr.readLine()) != null){
			StringTokenizer stk = new StringTokenizer(line, "\t");
			String mvid = stk.nextToken();
			String feats = stk.nextToken().substring(1);
			feats = feats.substring(0, feats.length() - 1);
			moviesMap.put(mvid, feats);
		}
		bfr.close();
		return moviesMap;
	}
	
	public static String getSortedFeatures(HashMap<Integer, Double>totalMap){
		
		TreeSet<Tup2> pairSet = new TreeSet<Tup2>(new Comparator<Tup2>() {
			public int compare(Tup2 t1, Tup2 t2){
				int res = t1.getIdx() < t2.getIdx() ? -1: 1;     //升序
				return res;
			}
		});
		
		for(Map.Entry<Integer, Double>entry : totalMap.entrySet()){
			Tup2 p2 = new Tup2(entry.getKey(), entry.getValue());
			pairSet.add(p2);
		}
		String out = "";
		for(Tup2 t2 : pairSet){
			out += t2.toString() + " ";
		}
		return out;
	}
	
	public static int[] getFeatIdx(String path)throws IOException{
		int[] ftidx = {0,0};  // movietags 起始id, join起始id； 
		BufferedReader bfr = new BufferedReader(new FileReader(path));
		String line = "";
		int cnt = 0;
		while((line = bfr.readLine()) != null){
			cnt++;
			int idx = line.indexOf(":");
			if(cnt == 3){
				ftidx[0] = Integer.parseInt(line.substring(idx + 1).trim());
			}else if (cnt == 4) {
				ftidx[1] = Integer.parseInt(line.substring(idx + 1).trim());
			}else if (cnt > 4) {
				break;
			}
		}
		bfr.close();
		return ftidx;
	}
	public static HashMap<Integer, Double>getmvFeatsMp(String featStr){
		HashMap<Integer, Double> mvftsMp = new HashMap<Integer, Double>();
		StringTokenizer stk = new StringTokenizer(featStr, ", ");
		while (stk.hasMoreTokens()) {
			Pair2 p2 = new Pair2(stk.nextToken());
			mvftsMp.put(p2.getIdx(), p2.getScore());
		}
		return mvftsMp;
	}
	public static double getDateDiff(double date1, double date2){
	
		double diff = Math.abs(date1 - date2);
		double res = 0.6;
		if(diff <= 0.2){
			res = 1.0;
		}else if (diff < 0.5 && diff > 0.2) {
			res = 0.6;
		}else if (diff >= 0.5 && diff < 0.7) {
			res = 0.3;
		}else if(diff >= 0.7){
			res = 0.1;
		}
		return res;
	}
	public static void main(String[] args)throws IOException{
		String moviePath = args[0];
		String inputPath = args[1];
		String outPth = args[2];
		int[] featIdx = getFeatIdx(args[3]);  // 
		int offset = featIdx[1] - 1; // featIdx[1] join交叉起始id
		int totlen = offset*2 - 1;   //   //交叉 无打分特征，有date特征
		BufferedWriter bfw = new BufferedWriter(new FileWriter(outPth));
		ConvertRelatedItem2FeaturesFullJoinwithDateDiffJoinPlus crift = new ConvertRelatedItem2FeaturesFullJoinwithDateDiffJoinPlus(moviePath);
		HashMap<String, String> mvMp = crift.movieItemsMp;
		BufferedReader bfr = new BufferedReader(new FileReader(inputPath));
		String line = "";
		while((line = bfr.readLine()) != null){
			StringTokenizer stk = new StringTokenizer(line);
			int cnt = Integer.parseInt(stk.nextToken());
			if(cnt > 1){    //去除样本中点击和展示次数为1 的样本（去燥）
				String mv1str = stk.nextToken();
				double lab = Double.parseDouble(mv1str.substring(0,1));
				String id1 = mv1str.substring(2);
				String id2 = stk.nextToken();
				if(mvMp.containsKey(id1) && mvMp.containsKey(id2)){
					HashMap<Integer, Double> ftmp1 = getmvFeatsMp(mvMp.get(id1));
					HashMap<Integer, Double> ftmp2 = getmvFeatsMp(mvMp.get(id2));
					
					for(Map.Entry<Integer, Double> entry : ftmp2.entrySet()){
						if(entry.getKey() < featIdx[1]- 2       //时间和打分
								&& ftmp1.containsKey(entry.getKey())){ //&& entry.getKey() >= featIdx[0]
							double tmpjoin = Math.log(1.0 * cnt) + 2;
							BigDecimal bd = new BigDecimal(tmpjoin);
							double res = Double.parseDouble(bd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
							ftmp1.put(entry.getKey() + offset, res);
						}else if (entry.getKey() == featIdx[1] - 1  //把 时间 也做交叉特征处理
								&& ftmp1.containsKey(entry.getKey())) {
							ftmp1.put(totlen, getDateDiff(entry.getValue(), ftmp1.get(entry.getKey())*cnt));
						}
						//BigDecimal bd = new BigDecimal(entry.getValue() * cnt);
						//double res = Double.parseDouble(bd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());		
						ftmp1.put(entry.getKey() + totlen, entry.getValue());
					}
					
					for(int i=0; i< cnt; i++){
						String out = getSortedFeatures(ftmp1);
						out = lab + " " + out;
						bfw.write(out);
						bfw.flush();
						bfw.newLine();
					}
				}
			}
		}
		bfw.close();
		bfr.close();
	}
}
