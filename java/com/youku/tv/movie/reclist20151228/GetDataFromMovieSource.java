package com.youku.tv.movie.reclist20151228;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;
import com.youku.tv.movie.reclist20151228.MovieDataMeta;
public class GetDataFromMovieSource {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		//输出的key、value
		static Text outkey = new Text();
		static Text outvalue = new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String info = value.toString();
			/*分别统计电影ID，标题，平分，简介，导演，主要演员，电影类型*/
			try {
				JSONObject jObject = new JSONObject(info);
				String id = jObject.getString("id");
				String title = jObject.getString("title");
				String rating = jObject.get("rating").toString();
				String intro = jObject.getString("intro").replaceAll("\t", "").replaceAll(" ", "");

				StringBuffer director = new StringBuffer();
				if (jObject.get("director").toString().length() > 1) {
					JSONArray dire = jObject.getJSONArray("director");
					for (int i = 0; i < dire.length(); i++) {
						director.append(",").append(dire.getJSONObject(i).getString("name"));
					}
				}
				StringBuffer actor = new StringBuffer();
				if (jObject.get("actor").toString().length() > 1) {
					JSONArray act = jObject.getJSONArray("actor");
					for (int i = 0; i < act.length(); i++) {
						actor.append(",").append(act.getJSONObject(i).getString("name"));
					}
				}

				StringBuffer type = new StringBuffer();
				if (jObject.get("type").toString().length() > 1) {
					JSONArray ty = jObject.getJSONArray("type");
					for (int i = 0; i < ty.length(); i++) {
						type.append(",").append(ty.getJSONObject(i).getString("name"));
					}
				}

				MovieDataMeta mDataMeta = new MovieDataMeta();
				mDataMeta.Setid(id);
				mDataMeta.Settitle(title.trim().replaceAll("\t", ""));

				if (type.length() > 1) {
					mDataMeta.Settype(type.substring(1).trim().replaceAll("\t", ""));
				}

				if (actor.length() > 1) {
					mDataMeta.Setactor(actor.substring(1).trim().replaceAll("\t", ""));
				}

				if (director.length() > 1) {
					mDataMeta.Setdiretor(director.substring(1).trim().replaceAll("\t", ""));
				}

				mDataMeta.Setintro(intro.trim().replaceAll("\t", ""));
				mDataMeta.Setrating(Integer.valueOf(rating));
                /*将id作为key，演员、导演、简介、平分、标签作为value输出*/
				outkey.set(id);
				outvalue.set(mDataMeta.ToValueString());
				context.write(outkey, outvalue);

			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		// conf.set("mapred.job.queue.name", "mouse");
		conf.set("mapred.reduce.parallel.copies", "25");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "tv movie:GetDataFromMovieSource");
		job.setJarByClass(GetDataFromMovieSource.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				MapClass.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
