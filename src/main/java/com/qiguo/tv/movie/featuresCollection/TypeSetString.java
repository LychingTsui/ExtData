package com.qiguo.tv.movie.featuresCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.youku.tv.json.JSONArray;
import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class TypeSetString {
	public static JSONArray getArray(String str) throws JSONException{
		
		JSONArray jsa = new JSONArray(str);
		return jsa;
	}
	
	public static void main(String[] args)throws IOException{
		String path = args[0];
		BufferedReader bfr = new BufferedReader(new FileReader(path));
		String line = "";
		System.out.println("0000");
		while((line = bfr.readLine()) != null){
			String info[] = line.split("\t");
			if(info[25].endsWith("listDisplay")){
				System.out.println("111111");
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
					System.out.println("1----");
					if(info[27].startsWith("{")){
						JSONObject js = null;
						try {
							js = new JSONObject(info[27]);
							String guid = js.getString("guid");
							if(js.has("list")){
								System.out.println(js.get("list").toString());
								
								String[] names = JSONObject.getNames(new JSONObject(js.getString("list")));
								int len = names.length;
								if(len > 0){
									String name = names[0];
									System.out.println(name);
									String subjs = new JSONObject(js.getString("list")).get(name).toString();
									JSONObject json = new JSONObject(subjs);
									String group = json.get("group").toString();
									String keyStr = group + "\t" + guid+ "\t" +"cardList";
									System.out.println(keyStr);
								}
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
