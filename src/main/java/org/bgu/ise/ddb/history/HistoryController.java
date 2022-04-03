/**
 * 
 */
package org.bgu.ise.ddb.history;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{
	private final static String itemsTable = "ProjecItems#";
	public final static String usersHistoryTable = "ProjectHistory#";
	public final static String usersTable = "ProjectNaorLee#";
	public final static String redis_url = "132.72.65.45";
	
	
	
	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	
	public boolean isExistUser(String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		Jedis jedis = new Jedis(redis_url);
		result = !(jedis.hgetAll(usersTable+username)).isEmpty();
		jedis.close();

		return result;

	}
	
	private boolean isExistsItem(String title) {
		boolean res = false;
			Jedis jedis = new Jedis(redis_url);
			res = !(jedis.hgetAll(itemsTable+title)).isEmpty();
			jedis.close();
		return res;
	}


	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		System.out.println(username+" "+title);

		try {
			if (!isExistUser(username) || !isExistsItem(title)) {
				System.out.println("user or item are not exists");
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
				return;
			}
			else {
				long timestamp =  new Date().getTime();
				Map<String,String> userHistory  = new HashMap<String, String>();
				userHistory.put("UserName", username);
				userHistory.put("Title", title);
				userHistory.put("Timestamp", "" + timestamp);
				Jedis jedis = new Jedis(redis_url);
				jedis.hmset(usersHistoryTable+username+"#"+title, userHistory);
				jedis.hmset(usersHistoryTable+title+"#"+username, userHistory);
				jedis.close();
				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			HttpStatus status = HttpStatus.CONFLICT;
			response.setStatus(status.value());
		}
		


	}
	
	
	
	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		Jedis jedis = new Jedis(redis_url);
		Set <String> history_items = jedis.keys(usersHistoryTable+username+"#*");
//		System.out.println(history_items.size());
		ArrayList<HistoryPair> historyPairsArray = new ArrayList<>();

		for (String string : history_items) {
			Map<String, String> userHistory = jedis.hgetAll(string);
			String title = userHistory.get("Title");
			String timestamp = userHistory.get("Timestamp");
			HistoryPair historyPair = new HistoryPair(title, new Date(Long.parseLong(timestamp)));
			historyPairsArray.add(historyPair);
		}
		
		jedis.close();
		HistoryPair[] result =  historyPairsArray.toArray(new HistoryPair[historyPairsArray.size()]);
		Arrays.sort(result, new SortbyTimeStamp());
	
		return result;
	}
	
	
	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		
		//:TODO your implementation
		Jedis jedis = new Jedis(redis_url);
		Set<String> history_items = jedis.keys(usersHistoryTable+title+"#*");
		System.out.println(history_items.size());
		ArrayList<HistoryPair> historyPairsArray = new ArrayList<>();

		for (String string : history_items) {
			Map<String, String> userHistory = jedis.hgetAll(string);
			String username = userHistory.get("UserName");
			String timestamp = userHistory.get("Timestamp");
			HistoryPair historyPair = new HistoryPair(username, new Date(Long.parseLong(timestamp)));
			historyPairsArray.add(historyPair);
		}
		
		jedis.close();
		HistoryPair[] result =  historyPairsArray.toArray(new HistoryPair[historyPairsArray.size()]);
		Arrays.sort(result, new SortbyTimeStamp());
	
		return result;
		
	}
	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){		
		
		ArrayList<User> users = new ArrayList<User>();
		HistoryPair[] usersHistoryPairList = getHistoryByItems(title);
		for (HistoryPair usersHistoryPair : usersHistoryPairList) {
			Jedis jedis = new Jedis(redis_url);
			Map<String, String>  userMap = jedis.hgetAll(usersTable+usersHistoryPair.getCredentials());
			try {
				if (isExistUser(usersHistoryPair.getCredentials())){
					String username = userMap.get("UserName");
					String firstName = userMap.get("FirstName");
					String lastName = userMap.get("LastName");
					users.add(new User(username,firstName,lastName));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			jedis.close();
		}
		return users.toArray(new User[users.size()]);
	} 
	
	
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2) {
		try {
			HistoryPair[] title1HistoryPair = getHistoryByItems(title1);
			HistoryPair[] title2HistoryPair = getHistoryByItems(title2);

			Set<String> usersT1 = ToUserNameList(title1HistoryPair);
			Set<String> usersT2 = ToUserNameList(title2HistoryPair);

			Set<String> unionList = new HashSet<String>(usersT1);
			unionList.addAll(usersT2);

			Set<String> intersection = new HashSet<String>(usersT1);
			intersection.retainAll(usersT2);

			if (unionList.size() == 0) {
				return 0;
			}
			System.out.println(intersection.size());
			System.out.println(unionList.size());

			return ((double) intersection.size()) / unionList.size();
		} catch (Exception e) {
			System.out.println(e);
		}
		return 0;
	}

	private Set<String> ToUserNameList(HistoryPair[] titleHistoryPairList) {
		Set<String> titleUsersList = new HashSet<String>();

		for (HistoryPair historyPair : titleHistoryPairList) {
			titleUsersList.add(historyPair.getCredentials());

		}
		return titleUsersList;
	}


}


class SortbyTimeStamp implements Comparator<HistoryPair>
{
	// Used for sorting in descending order of
	// roll number
	public int compare(HistoryPair b, HistoryPair a)
	{
		return (int) (a.getViewtime().getTime() - b.getViewtime().getTime());
	}
}
