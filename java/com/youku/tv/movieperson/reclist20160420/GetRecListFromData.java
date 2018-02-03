package com.youku.tv.movieperson.reclist20160420;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class GetRecListFromData {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		static Text outkey = new Text();
		static Text outvalue = new Text();
		static HashMap<String, String> map = new HashMap<String,String>();
		static HashMap<String, Double> typemap = new HashMap<String,Double>();
		static DecimalFormat df = new DecimalFormat("0.000");
		double interval = 0.05;
		/*演员、导演、类型三种截止标志位*/
		static int actorcutoff = 3, dirtorcutoff = 2, typecutoff = 0;
       /*map的input数据是douban_reclist*/
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String info = value.toString();
			try {
				JSONObject jObject = new JSONObject(info);
				String title = jObject.getString("title");
				/*判断是否是电影库中电影*/
				if (!map.containsKey(title)) {
					// context.write(new Text("1"), new Text(title));
					return;
				}

				if (map.containsKey(title)) {
					String[] data = map.get(title).split(":");
					String[] actor = data[0].split(",");
					String[] type = null;
					if (data.length > 1) {
						type = data[1].split(",");
					}

					if (jObject.get("recs").toString().length() > 1) {
						JSONArray resc = jObject.getJSONArray("recs");
						double sim = 1.0;
						for (int i = 0; i < resc.length(); i++) {
							JSONObject jo = resc.getJSONObject(i);
							double d = sim - interval * i;
							String name = jo.getString("name");
							if (map.containsKey(name)) {
								String[] temp = map.get(name).split(":");
								String[] dactor = temp[0].split(",");

								for (int j = 0; j < actor.length; j++) {
									for (int j2 = 0; j2 < dactor.length; j2++) {
										if (!actor[j].equals("null") && !dactor[j2].equals("null")
												&& actor[j].length() > 1 && dactor[j2].length() > 1) {
											if (!actor[j].contains("...")
													&& !dactor[j2].contains("...")) {
												if (!data[0].contains("成龙")
														&& temp[0].contains("成龙")) {
													break;
												}
												outkey.set(actor[j]);
												outvalue.set(dactor[j2] + ":" + df.format(d));
												context.write(outkey, outvalue);
											}
										}
									}
								}

								if (type != null && temp.length > 1) {
									String[] dtype = temp[1].split(",");
									for (int j = 0; j < type.length; j++) {
										double tsimA = d - 0.1 * j;
										for (int j2 = 0; j2 < dtype.length; j2++) {
											double tsimB = tsimA - 0.1 * j2;
											if (!type[j].equals("null")
													&& !dtype[j2].equals("null")
													&& type[j].length() > 1
													&& dtype[j2].length() > 1) {
												outkey.set("\2" + type[j].replaceAll(":", "").replaceAll("：", ""));
												if (tsimB > 0) {
													double tt = tsimB;
													if (typemap.containsKey(dtype[j2].replaceAll(":", "").replaceAll("：", ""))) {
														tt = d / typemap.get(dtype[j2].replaceAll(":", "").replaceAll("：", ""));
														outvalue.set(dtype[j2].replaceAll(":", "").replaceAll("：", "") + ":"
																+ df.format(tt));
														context.write(outkey, outvalue);
													}
												}
											}
										}
									}
								}
							}

						}
					}
				}
			} catch (JSONException e) {
				return;
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			/*获取interval actor type director的截取位*/
			interval = Double.valueOf(context.getConfiguration().get("interval"));
			actorcutoff = Integer.valueOf(context.getConfiguration().get("actorcutoff"));
			typecutoff = Integer.valueOf(context.getConfiguration().get("typecutoff"));
			dirtorcutoff = Integer.valueOf(context.getConfiguration().get("dirtorcutoff"));
            /*将tagdata的输入路径在集群中进行分发，处理该路径的数据*/
			Path[] filePathList = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path filePath : filePathList) {
				loadIndex(filePath.toString(), context);
			}
			super.setup(context);
		}

		private void loadIndex(String file, Context context) throws IOException,
				InterruptedException {
			/*分别获取缓存文件中的actor director 和type*/
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] info = line.split("\t");
				/*tagdata路径下有两段数据，此处判断是取的tagdata中的tagsarg*/
				if (info.length > 2) {
					MovieDataMeta meta = new MovieDataMeta(line);

					String[] actor = meta.Getactor().split(",");
					String[] dirtor = meta.Getdiretor().split(",");
					String[] type = meta.Gettags().split(",");

					StringBuffer sBuffer = new StringBuffer();
					int len = actor.length;
					/*获取演员的标签*/
					if (actorcutoff > 0 && actorcutoff < len) {
						len = actorcutoff;
					}
					for (int i = 0; i < len; i++) {
						if (actor[i].length() > 0) {
							sBuffer.append(",").append(actor[i]);
						}
					}
					/*获取导演的标签*/
					len = dirtor.length;
					if (dirtorcutoff > 0 && dirtorcutoff < len) {
						len = dirtorcutoff;
					}
					for (int i = 0; i < len; i++) {
						if (dirtor[i].length() > 0) {
							sBuffer.append(",").append(dirtor[i]);
						}
					}
					/*获取电影的类型信息*/
					len = type.length;
					if (typecutoff > 0 && typecutoff < len) {
						len = typecutoff;
					}
					/*判断标志位*/
					boolean bb = false;
					if (sBuffer.length() == 0) {
						bb = true;
					}

					boolean b = false;
					for (int i = 0; i < len; i++) {
						if (type[i].length() > 0) {
							if (!b) {
								/*首次添加标签*/
								sBuffer.append(":").append(type[i]);
								b = true;
							} else {
								/*继续添加标签*/
								sBuffer.append(",").append(type[i]);
							}
						}
					}
					if (sBuffer.length() > 1) {
						if (bb) {
							/*继续添加*/
							map.put(meta.Gettitle(), sBuffer.toString());
						} else {
							/*首次添加，去掉首字符，*/
							map.put(meta.Gettitle(), sBuffer.substring(1));
						}
					}
				}
				/*tagdata里的另外一份文件tagtime*/
				else {
					double d = Double.valueOf(info[1]);
					if (d > 2 && !isInteger(info[0])) {
						/*权重求取*/
						d = Math.pow(d, 1 / 1.5) / 10;
						if (d < 1.5) {
							d = 1.5;
						}
						typemap.put(info[0], d);
					}
				}
			}
		}
		/*验证一个字符串是否是double类型*/
		public static boolean isInteger(String value) {
			try {
				Double.parseDouble(value);
				// if (value.contains("."))
				return true;
				// return false;
			} catch (NumberFormatException e) {
				return false;
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		static Text outvalue = new Text();
		static DecimalFormat df = new DecimalFormat("0.000");
		int cutoff = 0;

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			HashMap<String, Double> map = new HashMap<String,Double>();

			for (Text text : values) {
				String[] info = text.toString().split(":");
				double d = Double.valueOf(info[1]);
				if (map.containsKey(info[0])) {
					d += map.get(info[0]);
				}
				map.put(info[0], d);
			}

			StringBuffer sBuffer = new StringBuffer();
			Object[] obj = map.keySet().toArray();
			for (int i = 0; i < obj.length; i++) {
				sBuffer.append(",").append(obj[i].toString()).append(":")
						.append(df.format(map.get(obj[i].toString())));
			}

			// outvalue.set(NormalizeRecList(Utils.sortRecList(sBuffer.substring(1),
			// cutoff, 1, ",", ":")));
			outvalue.set(Utils.sortRecList(sBuffer.substring(1), cutoff, 1, ",", ":"));
			context.write(key, outvalue);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			cutoff = Integer.valueOf(context.getConfiguration().get("cutoff"));
			super.setup(context);
		}

		private static String NormalizeRecList(String reclist) {
			String[] data = reclist.split(",");

			double max = Double.valueOf(data[0].split(":")[1]);
			double min = Double.valueOf(data[data.length - 1].split(":")[1]);
			double Max = 0.9, Min = 0.1;

			if (max != min) {
				StringBuffer sBuffer = new StringBuffer();
				for (int i = 0; i < data.length; i++) {
					String[] info = data[i].split(":");
					double sim = Double.valueOf(info[1]);
					double Sim = (Max - Min) / (max - min) * (sim - min) + Min;
					sBuffer.append(",").append(info[0]).append(":").append(df.format(Sim));
				}
				return sBuffer.substring(1);
			} else {
				StringBuffer sBuffer = new StringBuffer();
				for (int i = 0; i < data.length; i++) {
					String[] info = data[i].split(":");
					sBuffer.append(",").append(info[0]).append(":").append(0.5);
				}
				return sBuffer.substring(1);
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("interval", otherArgs[3]);
		conf.set("cutoff", otherArgs[4]);
		String[] data = otherArgs[5].split(":");
		conf.set("actorcutoff", data[0]);
		conf.set("dirtorcutoff", data[1]);
		conf.set("typecutoff", data[2]);

		Job job = Job.getInstance(conf, "tv movie:GetRecListFromData");

		Path cachePath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(cachePath);
		for (FileStatus fstatus : arr) {
			Path p = fstatus.getPath();
			if (fs.isFile(p)) {
				job.addCacheFile(p.toUri());
			}
		}

		job.setJarByClass(GetRecListFromData.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setReducerClass(ReducerClass.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
