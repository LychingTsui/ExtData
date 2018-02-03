package com.youku.tv.movieperson.reclist20160420;

import java.util.Comparator;

public class  tagss{
	public Double value;
	public String key;
	public tagss(String key,Double value){
		this.key=key;
		this.value=value;
	}
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	Comparator<tagss> comparator=new Comparator<tagss>() {

		public int compare(tagss o1, tagss o2) {
			tagss a1=(tagss) o1;
			tagss a2=(tagss) o2;
			int s=a1.getValue().compareTo(a2.getValue());
			if (s==0) {
				return a1.getKey().compareTo(a2.getKey());
			}
			else {
				return s;
			}
		}
	};
	
}
