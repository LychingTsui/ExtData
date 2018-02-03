package com.youku.tv.movieperson.reclist20160420;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	private String text;
	private String value;
   //构造方法
	public TextPair(String text, String value) {
		super();
		this.text = text;
		this.value = value;
	}

	public TextPair() {
		super();
		// TODO Auto-generated constructor stub
	}
  //写入数据
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(text);
		out.writeUTF(value);
	}
  //读取数据
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.text = in.readUTF();
		this.value = in.readUTF();
	}
   //重写比较方法
	public int compareTo(TextPair o) {
		// TODO Auto-generated method stub
		TextPair that = (TextPair) o;
		if (!this.text.equals(that.text)) {
			return this.text.compareTo(that.text);
		} else {
			return this.value.compareTo(that.value);
		}
	}
  
	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
