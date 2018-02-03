package com.youku.tv.movieperson.reclist20160420;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextComparator extends WritableComparator {

	@SuppressWarnings("rawtypes")
	@Override
	//构造比较器，比较两个类
	public int compare(WritableComparable a, WritableComparable b) {
		TextPair tp1 = (TextPair) a;
		TextPair tp2 = (TextPair) b;
		return tp1.getText().compareTo(tp2.getText());
	}

	public TextComparator() {
		super(TextPair.class, true);
	}

}
