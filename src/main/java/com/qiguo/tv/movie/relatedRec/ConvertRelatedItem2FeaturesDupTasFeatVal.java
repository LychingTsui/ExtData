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

public class ConvertRelatedItem2FeaturesDupTasFeatVal {
	HashMap<String, String>movieItemsMp = new HashMap<String, String>();
	public ConvertRelatedItem2FeaturesDupTasFeatVal(String path)
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
		int[] ftidx = {0,0};
		BufferedReader bfr = new BufferedReader(new FileReader(path));
		String line = "";
		int cnt = 0;
		while((line = bfr.readLine()) != null){
			cnt++;
			int idx = line.indexOf(":");
			if(cnt == 2){
				ftidx[0] = Integer.parseInt(line.substring(idx + 1).trim());
			}else if (cnt == 3) {
				ftidx[1] = Integer.parseInt(line.substring(idx + 1).trim());
			}else if (cnt > 3) {
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
	public static void main(String[] args)throws IOException{
		String moviePath = args[0];
		String inputPath = args[1];
		String outPth = args[2];
		int[] featIdx = getFeatIdx(args[3]);
		int offset = featIdx[1] - featIdx[0]; // offset为tags 键的长度,含上映和打分 
		int totlen = featIdx[1] + offset - 3; 
		BufferedWriter bfw = new BufferedWriter(new FileWriter(outPth));
		
		ConvertRelatedItem2FeaturesDupTasFeatVal crift = new ConvertRelatedItem2FeaturesDupTasFeatVal(moviePath);
		HashMap<String, String> mvMp = crift.movieItemsMp;
		BufferedReader bfr = new BufferedReader(new FileReader(inputPath));
		String line = "";
		while((line = bfr.readLine()) != null){
			StringTokenizer stk = new StringTokenizer(line);
			int cnt = Integer.parseInt(stk.nextToken());
			String mv1str = stk.nextToken();
			
			double lab = Double.parseDouble(mv1str.substring(0,1));
			String id1 = mv1str.substring(2);
			String id2 = stk.nextToken();
			if(mvMp.containsKey(id1) && mvMp.containsKey(id2)){
				HashMap<Integer, Double> ftmp1 = getmvFeatsMp(mvMp.get(id1));
				HashMap<Integer, Double> ftmp2 = getmvFeatsMp(mvMp.get(id2));
				
				for(Map.Entry<Integer, Double> entry : ftmp2.entrySet()){
					if(entry.getKey() < featIdx[1] - 2       //2:上映和时间
							&& entry.getKey() >= featIdx[0]
							&& ftmp1.containsKey(entry.getKey())){
						ftmp1.put(entry.getKey() + offset, 1.0);
					}
					BigDecimal bd = new BigDecimal(entry.getValue() * cnt);
					double res = Double.parseDouble(bd.setScale(2, BigDecimal.ROUND_HALF_UP).toString());		
					if(entry.getKey() < featIdx[0] || entry.getKey() > featIdx[1]-3){
						ftmp1.put(entry.getKey() + totlen, entry.getValue());
					}else {
						ftmp1.put(entry.getKey() + totlen, res);
					}	
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
		bfw.close();
		bfr.close();
	}
}
