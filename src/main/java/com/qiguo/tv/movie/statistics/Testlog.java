package com.qiguo.tv.movie.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class Testlog {
	
	public static JSONArray getArray(String str) throws JSONException{
		JSONArray jsa = new JSONArray(str);
		return jsa;
	}
	public static void main(String[] args)throws IOException{
		String path = "/Users/qiguo/Documents/testlog.txt";
		BufferedReader bfr = new BufferedReader(new FileReader(path));
		String line = "";
		while((line = bfr.readLine()) != null){
			String info[] = line.split("\t");
			if(info[25].endsWith("listDisplay")){
				if(info[26].equals("cardDetail")){
					if(info[27].startsWith("{")){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							if(js.has("list")){
								JSONArray jsarr = getArray(js.getString("list"));
								String group =jsarr.getJSONObject(0).getString("group");
								String guid = js.getString("guid");
							
								String keyStr = group+" "+guid+" "+ info[26];
								System.out.println(keyStr);
							}	
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
				}else if (info[26].equals("cardList")) {
					if(info[27].startsWith("{")){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							String guid = js.getString("guid");
							if(js.has("list")){
								
								String[] names = JSONObject.getNames(new JSONObject(js.getString("list")));
								String name = names[0].equals("personalRec") ? names[1]:names[0];
								String subjs = new JSONObject(js.getString("list")).get(name).toString();
								JSONArray jsonArr = getArray(subjs);
								String group = jsonArr.getJSONObject(0).get("group").toString();
								String keyStr = group + "\t" + guid+ "\t" +"cardList";
								System.out.println(keyStr);
							
							}
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
				}		
			}
		}
		bfr.close();
	}
}
