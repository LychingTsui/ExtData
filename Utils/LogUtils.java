package com.qiguo.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.youku.tv.json.JSONException;
import com.youku.tv.json.JSONObject;

public class LogUtils {
	private String vid = "";
	private String guid = "";
	private String visitedTime = "";
	private String duration = "";
	private String pt = "";
	public String getVid() {
		return vid;
	}
	public void setVid(String vid) {
		this.vid = vid;
	}
	public String getGuid() {
		return guid;
	}
	public void setGuid(String guid) {
		this.guid = guid;
	}
	public String getVisitedTime() {
		return visitedTime;
	}
	public void setVisitedTime(String visitedTime) {
		this.visitedTime = visitedTime;
	}
	public String getDuration() {
		return duration;
	}
	public void setDuration(String duration) {
		this.duration = duration;
	}
	public String getPt() {
		return pt;
	}
	public void setPt(String pt) {
		this.pt = pt;
	}
	public void parseValue(String str) {
		if (str == null || str.length() ==0) {
			return;
		}
		String [] items =StringUtils.splitPreserveAllTokens(str, "\t");
		if (items[21].startsWith("movie") && items.length == 26) {
            visitedTime = get_time(items[1]);
            guid = get_mac_from_guid(items[5]);
            vid = get_id(items[items.length-1]);    
		}
		else if (items[7].equals("movie") && items.length >= 28) {
			visitedTime = get_time(items[1]);
			guid = get_mac_from_guid(items[5]);
			vid  = get_vvid(items[26]);
			pt = items[23];
			duration = items[20];
		}
		
	}
	//获取电影id方法
	public static String get_id(String str){
			if(str == null || str.length() == 0)
				return null;
			try {
			      JSONObject object = new JSONObject(str);
			      if (object.has("id")) {
			        return object.get("id").toString();
			       }
			    }
			    catch (JSONException e)
			     {
			      e.printStackTrace();
			    }
			return "";
		}
	public String clickValue2String() {
		return "guid:" + guid + "\tmovieId:" + vid +"\tvisitedTime:" +visitedTime;
	}
	public static String get_mac_from_guid(String s){
		if(StringUtils.split(s,'.').length==2&&s.length()>17){
			return s.substring(0,17);//s.split("\\.").length
			}
		else if (StringUtils.split(s,'.').length>2&&StringUtils.split(s, ':').length>6) {
			return s;
		}
		return "";
	}
	//获取时间方法
	public static String get_time(String str){
		Date date=new Date(Long.valueOf(str));
		SimpleDateFormat format=new SimpleDateFormat("yyyyMMdd");
		String ret=format.format(date);
		return ret;
		}
	//获取电影的ID
	public static String get_vvid(String s){  
		if (s.equals("")) {
			return "";
		}
		try {
			if (s.startsWith("{")) {
				JSONObject object = new JSONObject(s);
				if (object.has("id")) {
					return object.get("id").toString();
					}
				}
				else {
					if (s.startsWith("\"id")) {
						s="{"+s+"}";
						JSONObject object = new JSONObject(s);
						return object.getString("id");
					}
				}
			}
		 catch (JSONException e){
				e.printStackTrace();
			}
			return "";
		}
		
	public static void main(String[] args) {
		String it = "1	1482422402079	115.197.96.241	浙江省	杭州市	E0:76:D0:E4:E8:5E.80:0B:51:04:10:5F.deviceId.serialNumber.96985572	1482422394312.013	72	3.7.7	Shafa	video	5.1.1	zh_CN	MStar%20Semiconductor%2C%20Inc.	monaco	XGIMI%20TV	unknown	E0%3A76%3AD0%3AE4%3AE8%3A5E	3809085	w1920_h1080	240	movie_最新更新		1	4	2";
		String md = "1	1482422402593	123.13.253.108	河南省	许昌市	00:87:41:82:03:1A.00:1A:34:EE:15:02.deviceId.serialNumber.15973495	1482422379866.112	72	3.7.7	Zndsdb	browser	4.2.2	zh_CN	Konka%2BGroup%2BCo.%2C%2BLtd	nike	Konka%2BAndroid%2BTV%2B818	unknown	00%3A87%3A41%3A82%3A03%3A1A	25930859	w1920_h1080	240	movie_最新更新		1	8	8";
		System.out.println(StringUtils.splitPreserveAllTokens(md, "\t")[21]);
	}
	
}
