package com.youku.tv.movieperson.reclist20160420;
public class MovieDataMeta {
	private String id = "";
	private int idindex = 0;
	private String title = "";
	private String type = "";
	private String actor = "";
	private String diretor = "";
	private String intro = "";
	private int rating = 0;
	private int args = 0;
	private String tags = "";
	private String date = "";

	public MovieDataMeta() {

	}

	public MovieDataMeta(String str) {
		parse(str);
	}

	public void parse(String str) {
		if (str == null || str.length() == 0) {
			return;
		}

		int index = str.indexOf("\t");

		id = str.substring(0, index);
		parseValue(str.substring(index + 1));
	}

	public void parseValue(String str) {
		if (str == null || str.length() == 0) {
			return;
		}
		String[] items = str.split("\t");
		if (items.length < 8) {
			return;
		}

		int index = items[0].indexOf(":");
		title = items[0].substring(index + 1);

		index = items[1].indexOf(":");
		type = items[1].substring(index + 1);

		index = items[2].indexOf(":");
		actor = items[2].substring(index + 1);

		index = items[3].indexOf(":");
		diretor = items[3].substring(index + 1);

		index = items[4].indexOf(":");
		intro = items[4].substring(index + 1);

		index = items[5].indexOf(":");

		rating = Integer.valueOf(items[5].substring(index + 1));

		index = items[6].indexOf(":");
		args = Integer.valueOf(items[6].substring(index + 1));

		index = items[7].indexOf(":");
		tags = items[7].substring(index + 1);
		index = items[8].indexOf(":");
		date = items[8].substring(index + 1);
		index = items[9].indexOf(":");
		System.out.println(index);
		idindex = Integer.valueOf(items[9].substring(index + 1));
	}

	public String Getid() {
		return id;
	}

	public void Setid(String id) {
		this.id = id;
	}

	public String Gettitle() {
		return title;
	}

	public void Settitle(String title) {
		this.title = title;
	}

	public String Gettype() {
		return type;
	}

	public void Settype(String type) {
		this.type = type;
	}

	public String Getactor() {
		return actor;
	}

	public void Setactor(String actor) {
		this.actor = actor;
	}

	public String Getdiretor() {
		return diretor;
	}

	public void Setdiretor(String diretor) {
		this.diretor = diretor;
	}

	public String Getintro() {
		return intro;
	}

	public void Setintro(String intro) {
		this.intro = intro;
	}

	public String Gettags() {
		return tags;
	}

	public void Settags(String tags) {
		this.tags = tags;
	}

	public int Getrating() {
		return rating;
	}

	public void Setrating(int rating) {
		this.rating = rating;
	}

	public int Getargs() {
		return args;
	}

	public void Setargs(int args) {
		this.args = args;
	}
	public String Getdate() {
		return date;
	}

	public void Setdate(String date) {
		this.date = date;
	}

	public int Getidindex() {
		return idindex;
	}

	public void Setidindex(int idindex) {
		this.idindex = idindex;
	}

	public String ToString() {
		return id + "\t" + ToValueString();
	}

	public String ToValueString() {
		return "title:" + title + "\ttype:" + type + "\tactor:" + actor + "\tdiretor:" + diretor
				+ "\tintro:" + intro + "\trating:" + rating + "\targs:" + args + "\ttags:" + tags
				+ "\tdate:" + date + "\tidindex:" + idindex;
	}

	public static void main(String[] arg) {
		String data = "e3079beb2a453fa9327bb1c3ba45d0d6	title:猛犸复活	type:	actor:Vincent Ventresca,Dr. Frank Abernathy,Summer Glau,Jack Abernathy,Tom Skerritt,Simon Abernathy	diretorTim Cox	intro:道电光划过长空并碰撞城自然历史博物馆。原来是一艘宇宙飞船，外来异兽为了适应地球环境，遂捉住了地球生物－－展馆内冰冻的巨大猛玛。冰冻的４万年前古老猛玛得到解救，巨大猛玛逃出博物馆凶性大发，整个城市瞬间陷入凶猛的铁足之下，到外一片悲呼惨叫，人们无处可逃……	rating:43	args:0	tags:";
		String data1="7fb4563d79e8ca5680d7e5eaefb34797	title:0.5毫米	type:剧情	actor:安藤樱,织本顺吉,木内みどり,土屋希望,井上竜夫,东出昌大,柳原晴郎,角替和枝,浅田美代子,坂田利夫,柄本明,草笛光子,津川雅彦	diretor:安藤桃子	intro:在某老人护理机构供职的山岸佐和（安藤樱饰）长久以来一直照顾生活不能自理的老人昭三（织本顺吉饰）。某天昭三的女儿向其提出一个匪夷所思的要求，虽然有些勉强，佐和还是应承了下来，谁曾想这一决定竟彻底改变了她的命运。突如其来的灾难让佐和丢掉了工作，进而流落街头。在四处闲逛的时候，她先后邂逅了康夫（井上龙夫饰）、阿茂（坂田利夫饰）、义男（津川雅彦饰）等有着各种各样怪癖和坏毛病的老年人。佐和就像恶意满满的魔女，不由分说闯入老人们的生活。她霸道地纠正老人们的恶习，却也为他们带来了久违的关怀……　　本片根据导演安藤桃子依据自身护理经验撰写的小说改编。	rating:81	args:6875	tags:日本,日本电影,安藤樱,剧情,2014,安藤サクラ,日影,安藤モモ子	date:2014	idindex:6502";
		System.out.println(data1.split("\t").length);
		MovieDataMeta meta = new MovieDataMeta(data1);
		
		System.out.println(meta.Gettitle());
		System.out.println(meta.Getid());
		System.out.println(meta.Getrating());
		System.out.println(meta.Getdate());
	}


	}

