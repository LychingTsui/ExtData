package recommend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.TreeMultimap;

public class RecommendTest {
 /*
 * test = {'A':set(['a','b','d']),'B':set(['a','c']),'C':set(['b','e']),'D':set(['a','d','c','e'])}
 * def Cos_like_plus(testdata):
    #首先，建立一个物品－用户的倒排表
    item_user = {}
    for user,items in testdata.iteritems(): #我们对每一个用户中
        for i in items: #该用户对每一个物品
            #print i
            item_user.setdefault(i,set())#开始初始化，如果已经初始化，不做操作
            item_user[i].add(user)  #建立一个物品到用户的映射，也就是对a物品来说，它同时被A，B二者拥有，所以 item_user['a'] = set(['A','B'])

    #接下来，我们建立一个用户所拥有的物品长度列表，因为计算Coslike需要知道A，B俩用户各自的物品列表长度
    itemlist_len = {}
    #同时，建立相似度矩阵
    like_M = {}

    for item,users in item_user.iteritems():
        for i in users:
            #print i
            like_M.setdefault(i,{})
            itemlist_len.setdefault(i,0)
            itemlist_len[i] += 1        #对每一个item,它的拥有者计数+1
            for j in users:
                if i == j:
                    continue        #计算除自己外，每个用户俩俩之间相似度
                like_M[i].setdefault(j,0)
                like_M[i][j] += 1   #但是，这里like_M[i][j]结果只是AB之间的cos like的分子，也就是A&B，下面我们要计算出分母并最终得到矩阵W

   */
	
	public static Map<String, ArrayList<String>> coslike(Map<String, ArrayList<String>> map1){
		Map<String, ArrayList<String>> item_user=new HashMap<String, ArrayList<String>>();//构建用户－物品矩阵
		Map<String, ArrayList<String>> matrix=new HashMap<String, ArrayList<String>>();//用户电影推荐的相似矩阵
		Map<String, Integer>itemLength=new HashMap<String, Integer>();//统计一个用户已经观看的电影数量长度，用于余弦相似度计算
		ArrayList<String>list=null;//储存用户集合
		for (Entry<String, ArrayList<String>>maps:map1.entrySet()) {
			String user=maps.getKey();
			//System.out.println(user);
			ArrayList<String>item=maps.getValue();
			//System.out.println(item);
			for (String string : item) {
				if (item_user.containsKey(string)) {
					ArrayList<String>rep=item_user.get(string);
					rep.add(user);
					item_user.put(string, rep);
				}
				else{
					list=new ArrayList<String>();
					list.add(user);
					item_user.put(string, list);
				}
			}
			//System.out.println(item_user);
		}
		for (Entry<String, ArrayList<String>>maps:item_user.entrySet()) {
			String item=maps.getKey();
			ArrayList<String>users=maps.getValue();
			for (int i=0;i<users.size();i++) {
				if (itemLength.containsKey(users.get(i))) {
					int a =itemLength.get(users.get(i))+1;
					itemLength.put(users.get(i), a);
				}
				else{
					itemLength.put(users.get(i), 1);
				}
				for (int j = 0; j < users.size(); j++) {
					if (i==j) {
						continue;
					}
					else{
						if (matrix.containsKey(users.get(i))) {
							ArrayList<String>user_list=matrix.get(users.get(i));//获取当前用户的的相似用户以及次数
							HashMap<String,String>repe=new HashMap<String,String>();//记录一个用户的相似用户之间的次数
							for (String string : user_list) {
								String bd[]=string.split(":");
								repe.put(bd[0],bd[1]);	
							}
							if (repe.containsKey(users.get(j))) {
								String count=(Integer.valueOf(repe.get(users.get(j)))+1)+"";
								repe.put(users.get(j),count);
							}
							else{
								repe.put(users.get(j),"1");
							}
							user_list.clear();
							for (Entry<String, String> map:repe.entrySet()) {
								user_list.add(map.getKey()+":"+map.getValue());
							}
							matrix.put(users.get(i), user_list);
						}
						else{
							ArrayList<String>user_list=new ArrayList<String>();
							user_list.add(users.get(j)+":1");
							matrix.put(users.get(i), user_list);
							
						}
					}	
				}
			}
		}
		//System.out.println(matrix);
		Map<String, ArrayList<String>>coMatrix=new HashMap<String, ArrayList<String>>();//存储用户之间的相似度
		for (Entry<String, ArrayList<String>> map : matrix.entrySet()) {
			ArrayList<String>listd=new ArrayList<String>();//用于统计计算结果的列表
			ArrayList<String> couser=map.getValue();
			for (String string : couser) {
				String bb[]=string.split(":");
				 double cc=Double.valueOf(bb[1]);
				 //System.out.println(itemLength.get(map.getKey())+map.getKey());
				double bbt=cc/Math.sqrt(Integer.valueOf(itemLength.get(map.getKey()))*Integer.valueOf(itemLength.get(bb[0])));
				listd.add(bb[0]+":"+bbt);
				//System.out.println(listd);
			}
			listd=sortRecList(listd, 3);
			//System.out.println(listd);
			coMatrix.put(map.getKey(), listd);
			System.out.println(coMatrix);
			//{'A': {'C': 1, 'B': 1, 'D': 2}, 'C': {'A': 1, 'D': 1}, 'B': {'A': 1, 'D': 2}, 'D': {'A': 2, 'C': 1, 'B': 2}}
		}
		return matrix;
	}
	//list 集合倒叙排列
	public static ArrayList<String> sortRecList(ArrayList<String> recList, int cutoff) {
		ArrayList<String> linkList = new ArrayList<String>();
		TreeMultimap<Double, String> recResultMultimap = TreeMultimap.create(
				Collections.reverseOrder(), Collections.reverseOrder());
		for (String cfResult : recList) {
			String[] members = cfResult.split(":");
			if (members.length == 2) {
				recResultMultimap.put(Double.parseDouble(members[1]), cfResult);
			}
		}
		Set<Double> keyDoubles = recResultMultimap.keySet();
		Iterator<Double> iterator = keyDoubles.iterator();
		int count = 0;
		while (iterator.hasNext()) {
			Double confidence = iterator.next();
			Collection<String> collection = recResultMultimap.get(confidence);
			for (String c : collection) {
				linkList.add(c);
				count++;
				if (count == cutoff) {
					//System.out.println(linkList);
					return linkList;
					
				}
			}
		}
		return linkList;

	}
	public static void main(String[] args) {
		ArrayList<String>list1=new ArrayList<String>();
		ArrayList<String>list2=new ArrayList<String>();
		ArrayList<String>list3=new ArrayList<String>();
		ArrayList<String>list4=new ArrayList<String>();
//		list.add("A:0.58");
//		list.add("B:0.92");
//		list.add("C:0.32");
//		list.add("D:0.34"); 
		HashMap<String, ArrayList<String>> map=new HashMap<String, ArrayList<String>>();
	
		// test = {'A':set(['a','b','d']),'B':set(['a','c']),'C':set(['b','e']),'D':set(['a','d','c','e'])}
		list1.add("a");
		list1.add("b");
		list1.add("d");
		map.put("A", list1);
		//list2.clear();
		list2.add("a");
		list2.add("c");
		map.put("B", list2);
		//list3.clear();
		list3.add("b");
		list3.add("e");
		map.put("C", list3);
		//list.clear();
		list4.add("a");
        list4.add("d");
        list4.add("c");
        list4.add("e");
        map.put("D",list4);
        //list.clear();
      RecommendTest.coslike(map);
		//RecommendTest.sortRecList(list, 3);
	}
	/*#这时，物品长度列表已经创建完成
    W = {}
    #接下来计算分母并返回最后的余弦相似度矩阵
    for itemA,v in like_M.iteritems():
        #print itemA,v
        W.setdefault(itemA,{})
        for itemB,cnt in v.iteritems():
            #print cnt
            W[itemA].setdefault(itemB,0)
            W[itemA][itemB] = cnt / math.sqrt(itemlist_len[itemA] * itemlist_len[itemB])    #计算每一个余弦相似度

    print W
    return W


#好，我们通过上面得到最后的相似矩阵，这样我们可以根据它进行推荐了，下面我们实现给用户A推荐和他相似的前K个用户都看了什么（但是A没看过都－ －）

def UserBased_Commend(user,testdata,W,K):   #输入分别为需要进行推荐都目标用户A,样本训练原数据，相似矩阵W和K值
    rank = {} #我们需要进行排序取前K
    watched_movie = testdata[user]  #根据定义，我们需要过滤掉目标用户A看过的电影
    for user,cnt in sorted(W[user].iteritems(),key=itemgetter(1),reverse=True)[:K]: #按照二维字典user那一行，第1个数据域(从0开始),降序排列
        for i in testdata[user]:
            if i in watched_movie:  #过滤A看过的电影
                continue
            rank.setdefault(i,0)
            rank[i] += cnt #本来这里我们cnt还需要乘以一个权重，这里简单认为用户所有反馈都是隐形反馈，权重默认都是1
    return rank

#先来实验一下

# t = Cos_like(test)
t = Cos_like_plus(test)
for i,v in t.iteritems():
    for j,cnt in v.iteritems():
        if i != j:
            print i+':'+j+' ~ %.2f'%cnt+"\t",
    print
#没问题。

#实验一下。。。
rk = UserBased_Commend('A',test,t,2)    #推荐和A最像的前2个用户，我们这里仅有ABCD四个用户，因为和A最相近的是C和B用户，所以给A推荐C，B有的且A没有的
print rk    #看到给A推荐的是c和e ，A本身有abd，C有ac，B有de，去掉ad，推荐ce。结束。*/
}
