package com.qiguo.tv.movie.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.youku.tv.json.JSONObject;

public class PageUrl {
	public static void main(String[] args) throws IOException{
		
		String in = "/Users/qiguo/Documents/log.txt";
		BufferedReader bfr = new BufferedReader(new FileReader(in));
		String line = "";
		while((line = bfr.readLine())!= null){
			String ver = "";
			
			String[] infoStr = line.split("\t", -1);
			if(infoStr.length < 27){
				return;
			}
			if(infoStr[27].startsWith("{")){
				if(infoStr[25].startsWith("/首页/浏览")){
					try {
						JSONObject json = new JSONObject(infoStr[27]);
						String guid = infoStr[15];
						if(guid.isEmpty()){
							guid = json.getString("guid");
						}
						
						if(json.has("info")){
							String jsinfo = json.getString("info");
						
							if(jsinfo.startsWith("{")){
								JSONObject subjs = new JSONObject(jsinfo);
								if(subjs.has("actionItem")){
									JSONObject  js2 = new JSONObject(subjs.get("actionItem").toString());
									if(js2.has("webUrl") && !js2.isNull("webUrl")){
										String url = js2.getString("webUrl");
										System.out.println(url);
										ver = getVer(infoStr);
										if(ver.startsWith("5") ){ //&&!url.contains("qiguo")
											//&& !url.contains("tvall.cn") 
											//&& !url.contains("tvapk.net") 
											//&& !url.startsWith("file:///")
											String urlstr = url ;
											//System.out.println(urlstr);
										}
									}
									
								}
							}
						}
					} catch (Exception e) {
						// TODO: handle exception
						e.printStackTrace();
					}
				}
			}
			
		}
		bfr.close();
	}
	public static String getVer(String[] strs){
		String vsr ="";
		for(int i = 0; i< strs.length; i++){
			if(strs[i].startsWith("电视家") || strs[i].startsWith("電視家")){
				for(int j = i+1; j<strs.length; j++){
					if(!strs[j].trim().isEmpty() ){
						vsr = strs[j];
						break;
					}
				}
			}
		}
		return vsr;
	}
}
