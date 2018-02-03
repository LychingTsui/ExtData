package com.youku.tv.movie.reclist20151228;

import org.apache.commons.lang.StringUtils;
// movies本身属性信息
public class MovieDataMeta {
	private String id = "";
	private String title = "";
	private String type = "";
	private String actor = "";
	private String diretor = "";
	private String intro = "";
	private int rating = 0;
	private int args = 0;
	private String tags = "";
	private int idindex = 0;
	private String date = "";
	private String duration="";
	private String area="";
	public String Getdate() {
		return date;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}
	public String getDuration() {
		return duration;
	}

	public void Setdate(String date) {
		this.date = date;
	}
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
        if (index>0) {
        	id = str.substring(0, index);
    		parseValue(str.substring(index + 1));
		}
		
	}

	public void parseValue(String str) {
		if (str == null || str.length() == 0) {
			return;
		}
		String[] items = StringUtils.splitPreserveAllTokens(str, "\t");
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
		index = items[10].indexOf(":");
		System.out.println(index);
		duration = items[10].substring(index + 1);
		index=items[11].indexOf(":");
		area=items[11].substring(index+1);
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
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

	public String ToString() {
		return id + "\t" + ToValueString();
	}
	public int Getidindex() {
		return idindex;
	}

	public void Setidindex(int idindex) {
		this.idindex = idindex;
	}

	public String ToValueString() {
		return "title:" + title + "\ttype:" + type + "\tactor:" + actor + "\tdiretor:" + diretor
				+ "\tintro:" + intro + "\trating:" + rating + "\targs:" + args + "\ttags:" + tags+ "\tdate:" + date +"\tidindex:" + idindex+"\tduration:" + duration+"\tarea:" + area;
	}

	public static void main(String[] arg) {
		String data = "e3079beb2a453fa9327bb1c3ba45d0d6	title:猛犸复活	type:	actor:Vincent Ventresca,Dr. Frank Abernathy,Summer Glau,Jack Abernathy,Tom Skerritt,Simon Abernathy	diretorTim Cox	intro:道电光划过长空并碰撞城自然历史博物馆。原来是一艘宇宙飞船，外来异兽为了适应地球环境，遂捉住了地球生物－－展馆内冰冻的巨大猛玛。冰冻的４万年前古老猛玛得到解救，巨大猛玛逃出博物馆凶性大发，整个城市瞬间陷入凶猛的铁足之下，到外一片悲呼惨叫，人们无处可逃……	rating:43	args:0	tags:";
		String data1="7fb4563d79e8ca5680d7e5eaefb34797	title:0.5毫米	type:剧情	actor:安藤樱,织本顺吉,木内みどり,土屋希望,井上竜夫,东出昌大,柳原晴郎,角替和枝,浅田美代子,坂田利夫,柄本明,草笛光子,津川雅彦	diretor:安藤桃子	intro:在某老人护理机构供职的山岸佐和（安藤樱饰）长久以来一直照顾生活不能自理的老人昭三（织本顺吉饰）。某天昭三的女儿向其提出一个匪夷所思的要求，虽然有些勉强，佐和还是应承了下来，谁曾想这一决定竟彻底改变了她的命运。突如其来的灾难让佐和丢掉了工作，进而流落街头。在四处闲逛的时候，她先后邂逅了康夫（井上龙夫饰）、阿茂（坂田利夫饰）、义男（津川雅彦饰）等有着各种各样怪癖和坏毛病的老年人。佐和就像恶意满满的魔女，不由分说闯入老人们的生活。她霸道地纠正老人们的恶习，却也为他们带来了久违的关怀……　　本片根据导演安藤桃子依据自身护理经验撰写的小说改编。	rating:81	args:6875	tags:日本,日本电影,安藤樱,剧情,2014,安藤サクラ,日影,安藤モモ子	date:2014     idindex:6502";
		String data2="95ee27c513d7b32a97579b5b0f0a3542	title:请叫我英雄	type:喜剧	actor:吴镇宇,乔任梁,林雪,关颖,钟丽缇,郑则仕,高捷,施予斐,蓝燕,张子栋	diretor:傅咏	intro:在老街以回收废物为生的彭达（乔任梁饰）与龙哥（林雪饰）做梦也没有想到一封遗书将二人地位华丽逆转，彭达一跃成为五星级大酒店的继承人，龙哥更是每天吃喝玩乐美女相伴。欧阳先生义子Danny（吴镇宇饰），在酒店摸爬滚打多年，冷酷无情，为酒店集团利益不择手段。Danny假意让彭达参与酒店事务，暗中与彭达上演一次次的明争暗斗，为了抢夺生意双方争的是焦头烂额。Danny以董事会的名义派遣各路人马调查彭达身份，只为等待时机将其一举歼灭，Danny没有想到的是早已苏醒的欧阳（郑则仕饰）暗中观察着自己的一举一动。一直懦弱的彭达对龙哥言听计从，但为了老街街坊和他所爱的凉茶铺盲女阿英，他开始违背龙哥对抗Danny，瞬间废物变英雄，万夫莫挡……	rating:47	args:2383	tags:喜剧,吴镇宇,乔任梁,国产电影,2012,中国电影,中国,钟丽缇";
		String data3="7dce0d37fd1febe19f3a6c6487d00f46	title:请叫我英雄	type:动作,恐怖	actor:大泉洋,有村架纯,长泽雅美,片濑那奈,吉泽悠,冈田义德,德井优	diretor:佐藤信介	intro:人气僵尸题材漫画《请叫我英雄》已确定开拍真人版，这部漫改电影将由大泉洋主演，有村架纯、长泽雅美出演，并由佐藤信介导演掌镜拍摄。　　《请叫我英雄》的舞台为现代日本，剧情中ZQN（片中僵尸的名称）突然出现，世界陷入一片混乱，此时平凡的主人公突然以英雄的姿态进行战斗，保卫自己的家园，是一部颇有人气的漫画作品。此次的改编将复制漫画中恐怖与紧张交织的各种元素，着重描写普通人挺身而出，逐渐成长为“英雄”的故事。此次选定的主角大泉洋向来擅长种类型的角色，他的出演也将为作品增添不少看点。该剧的女主角将由《小海女》一剧中脱颖而出的有村架纯担任，而有护士相关知识，行动大胆的主要角色薮将由长泽雅美饰演。　　这部新片将在韩国一家废弃的购物中心取景，同时片中会有不少动作场景，发行方东宝宣称这将是一部超大制作的“B级片”。	rating:74	args:2383	tags:喜剧,吴镇宇,乔任梁,国产电影,2012,中国电影,中国,钟丽缇";
		String data4="004ca49e81ec12d21e317bac22c95d98	title:致命游戏	type:剧情,悬疑,犯罪	actor:Alexandra Neldel,Clemens Schick,Max von Pufendorf	diretor:Manuel Flurin Hendry	intro:经历了一次绑架案后的律师安娜准备开始新的生活，为此她找到了一份新的工作——律师热线接线员。但某天安娜接到了一个神秘陌生人的来电，从而又卷入了一宗连环杀人案件。一切似乎正如杀手计划的那样。在这个危险致命的猫捉老鼠的游戏中，安娜不仅使自己面临着危险，也使朋友陷入险境。为了揭发真相，安娜勇敢地面对着变态凶手的操控……	rating:46	args:0	tags:	idindex:0";
		MovieDataMeta meta = new MovieDataMeta(data4);
		System.out.println(meta.ToValueString());
		System.out.println(meta.args+meta.idindex);
		System.out.println(meta.Gettitle());
		System.out.println(meta.Getid());
		System.out.println(meta.Getrating());
	}
}
