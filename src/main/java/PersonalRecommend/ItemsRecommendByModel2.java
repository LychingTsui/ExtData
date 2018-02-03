package PersonalRecommend;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.qiguo.tv.movie.featuresCollection.Pair2;
/*
 *@param  
 *@info : 存在交叉特征则为1，否则为0，
 *ItemsRecommendByModel 处理方式： 交叉特征若存在写入交叉特征对应的movieTags的对应特征值
 */

public class ItemsRecommendByModel2 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		HashSet<String> guidSet = new HashSet<String>();  
		HashMap<String, String>movieItemsFetMap = new HashMap<String, String>();
		HashMap<Integer, Double>model = new HashMap<Integer, Double>();
		int[] featIdx = new int[3]; //featidx[0] 电影tags起始Id，featidx[1] jointags 起始Id，featid[2]偏好特征起始id
		private int flag = -1;   // 标记model文件label 第一位为0： flag ＝0；为1: flag=1;
		private String group = "";
		protected void map(LongWritable key, Text val, Context context)throws IOException,
		InterruptedException{
			int topk = 200;
			StringTokenizer sTok = new StringTokenizer(val.toString(), "\t");		
			String guid = sTok.nextToken();
			if (guidSet.contains(guid)) {
				TreeSet<T2> canset = new TreeSet<T2>(new Comparator<T2>() {
		            public int compare(T2 p1, T2 p2){
		                int res = p1.getVal() > p2.getVal()? -1: 1;     ///> ：降序  < :  升序
		                return res;
		            }
		        });
				String likeStr = sTok.nextToken();
				likeStr = likeStr.substring(1);
				likeStr = likeStr.substring(0, likeStr.length()-1);
				for(Map.Entry<String, String>entry : movieItemsFetMap.entrySet()){
					String row = entry.getValue()+","+likeStr;
					Pair2[] feat = getFeature(row);
					double sc = predScore(feat, model);
					sc = flag == 1? sc : -sc;
					double logres = Math.log(sc+40.0)/Math.log(50);
					BigDecimal bgd = new BigDecimal(logres);
					double sctmp = Double.parseDouble(bgd.setScale(4, BigDecimal.ROUND_HALF_UP).toString());
					 if(canset.size() >= topk){
                        if(sctmp > canset.last().getVal()){   // 取top
                            canset.pollLast();
                            canset.add(new T2(entry.getKey(), sctmp));
                        }
                    }else {
                        canset.add(new T2(entry.getKey(), sctmp));
                    }
				}
				String outStr = "";
				int cnt = 1;
				for(T2 t2:canset){					
					if(cnt == canset.size()){
						outStr += t2.toString() + ":" + group;
					}else {
						outStr += t2.toString() + ":" + group + ",";
					}
					cnt++;
				}
				context.write(new Text(guid), new Text(outStr));
			}		
		}
		/**处理过程同ItemsRecommendByModel，**/
		public Pair2[] getFeature(String str){
			StringTokenizer stk = new StringTokenizer(str, " ,");			
	        HashSet<Pair2> set = new HashSet<Pair2>();
	        HashMap<Integer,Double> midMap = new HashMap<Integer, Double>();

	        while(stk.hasMoreTokens()){
	            String tmp = stk.nextToken();
	            Pair2 p2 = new Pair2(tmp);
	            if(p2.getIdx() >= featIdx[0] && p2.getIdx() < featIdx[2] ){
	                midMap.put(p2.getIdx(), p2.getScore());
	            }
	            set.add(p2);
	        }
	        
	        int deleted = 0;
	        int offset = featIdx[1] - featIdx[0];
	        Iterator<Map.Entry<Integer, Double>> iter = midMap.entrySet().iterator();
	        while(iter.hasNext()){
	            Map.Entry<Integer, Double>entry = iter.next();
	            if(entry.getKey() >= featIdx[1] && entry.getKey() < featIdx[2]){
	                int keyId = entry.getKey() - offset;
	                if(!midMap.containsKey(keyId)){    //交叉特征的特征值的处理
	                		iter.remove();
	                		++deleted;
	                }
	            }
	        }
	        
	        int featNodeNum = set.size();
	        int featTotal = featNodeNum - deleted;
	        Pair2[] feat = new Pair2[featTotal];
	        int i = 0;
	        for(Pair2 p : set){
	            if(p.getIdx() >= featIdx[1] && p.getIdx() < featIdx[2] ){
	                if(midMap.containsKey(p.getIdx())){
	                    feat[i++] = new Pair2(p.getIdx(), midMap.get(p.getIdx()));
	                }
	            }else{
	                feat[i++] = new Pair2(p.getIdx(), p.getScore());
	            }
	        }
	        return feat;
		}
		/**计算model下为 w*x +b 作为排序依据**/
		public double predScore(Pair2[] feature, HashMap<Integer, Double> model){
	        double res = 0.0;
	        for(Pair2 p2 : feature){
	        		res += model.get(p2.getIdx()) * p2.getScore();
	        }
	        BigDecimal bd = new BigDecimal(res);
	        double valp = Double.parseDouble(bd.setScale(4, BigDecimal.ROUND_HALF_UP).toString());
	        return valp;
	    }
	    
		public void setup(Context context)throws IOException,
		InterruptedException{
			flag = Integer.parseInt(context.getConfiguration().get("flag"));
			group = context.getConfiguration().get("group");
			Path[] filePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path : filePaths){
				loadData(path.toString(), context);
			}
			
			super.setup(context);
		} 
		/**处理方式同ItemsRecommendByModel**/
		public void loadData(String path, Context context)throws IOException,
		InterruptedException{
			FileReader fr = new FileReader(path);
			BufferedReader br = new BufferedReader(fr);
			String line = null;		
			while((line = br.readLine()) != null){
				if(line.startsWith("uid")){
					String guid = line.substring(3);
					guidSet.add(guid);
				}else if(line.startsWith("movieTags")){
					String[] movieTags = line.split(":");
					int movieTagsIdx = Integer.parseInt(movieTags[1].trim());
					featIdx[0] = movieTagsIdx;
				}else if(line.startsWith("joinLike")){
					String[] joinLikeStr = line.split(":");
					int joinLikesIdx = Integer.parseInt(joinLikeStr[1].trim());
					featIdx[1] = joinLikesIdx;
				}else if (line.startsWith("user")) {
					String[] userlikeId = line.split(":");
					int usrlikesId = Integer.parseInt(userlikeId[1].trim());
					featIdx[2] = usrlikesId;
				}else if (line.startsWith("w")) {
					String weight = line.substring(1);
					StringTokenizer stk = new StringTokenizer(weight);
					int idx = Integer.parseInt(stk.nextToken());
					double val = Double.parseDouble(stk.nextToken());
					model.put(idx, val);
				}else if (line.split("\t")[0].length() == 32) {
					String mvid = line.split("\t")[0];
					String mvFeatStr = line.split("\t")[1].substring(1);
					mvFeatStr = mvFeatStr.substring(0, mvFeatStr.length()-1);
					movieItemsFetMap.put(mvid, mvFeatStr);
				}					
			}			
			br.close();
		}
	}
	
		public static void main(String[] args)throws IOException,
		InterruptedException,ClassNotFoundException{
			
			Configuration conf = new Configuration();
			
			conf.set("mapred.reduce.parallel.copies", "25");
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			conf.set("flag", otherArgs[3]);
			conf.set("group", otherArgs[4]);
			Job job = Job.getInstance(conf, "topK");

			Path cachePath = new Path(otherArgs[2]);
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] arr = fs.listStatus(cachePath);
			for (FileStatus fstatus : arr) {
				Path p = fstatus.getPath();
				if (fs.isFile(p)) {
					job.addCacheFile(p.toUri());
				}
			}
		
			job.setJarByClass(ItemsRecommendByModel2.class);
			MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
					MapClass.class);
			
			job.setNumReduceTasks(1);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
