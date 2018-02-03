package PersonalRecommend;

public class T2 {
	String key;
	Double val;
	
	public T2(String str){
		int idx = str.indexOf(":");
		this.key = str.substring(0,idx);
		this.val = Double.parseDouble(str.substring(idx+1)) ;
	}
	public T2(String item, Double val){
		this.key = item;
		this.val = val;
	}
	public T2(String item, Integer val){
		this.key = item;
		this.val = Double.parseDouble(val.toString());
	}
	public String getKey(){
		return key;
	}
	public Double getVal(){
		return val;
	}
	
	public String toString(){
		return key + ":" + val;
	}
	
	//for test
	public static void main(String[] args){
		T2 t1 = new T2("123",34.0);
		System.out.println(t1);
	}
	
}
