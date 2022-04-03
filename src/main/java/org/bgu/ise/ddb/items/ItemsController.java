/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


import javax.servlet.http.HttpServletResponse;

import org.apache.jasper.tagplugins.jstl.core.Set;
import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import redis.clients.jedis.Jedis;



/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	
	private Connection connection =null;
	private final String username="naorsa";
	private final String password="abcd";
	private final String connectionUrl="jdbc:oracle:thin:@132.72.65.216:1521/oracle";
	private final String driver="oracle.jdbc.driver.OracleDriver";
	private final static String itemsTable = "ProjectItems#";
	private final static String redis_url = "132.72.65.45";
	
	private void ConnectToOracleDB()
	{
		System.out.println("was here");
		try 
		{
			Class.forName(this.driver); //registration of the driver
			this.connection = DriverManager.getConnection(this.connectionUrl, this.username, this.password);
			this.connection.setAutoCommit(false);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		ResultSet rs =null;
		PreparedStatement ps =null;
		List<MediaItems> mediaItemsList = new ArrayList<MediaItems>();
		try {
			ConnectToOracleDB();
			String query = "SELECT title,prod_year FROM MediaItems";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			while (rs.next()) {
				MediaItems media = new MediaItems(rs.getString(1), rs.getInt(2));
				mediaItemsList.add(media);
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		for (MediaItems m : mediaItemsList) {
			if(!isItemExists(m.getTitle()))
			{
				addMediaToJedis(m);
			}
			System.out.println(m.toString());
		}

		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	private boolean isItemExists(String title) {
		boolean result = false;
		Jedis jedis = new Jedis(redis_url);
		result = !(jedis.hgetAll(itemsTable+title)).isEmpty();
		jedis.close();
		return result;
	}




	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		
		URL url = new URL(urladdress);
		BufferedReader br = null;
		String line = "";

		try {
			br = new BufferedReader(new BufferedReader(new InputStreamReader(url.openStream())));
			while ((line = br.readLine()) != null) {
				String[] media = line.split(",");
				System.out.println(line);
				String title= media[0];
				String year=media[1];
				try {
					if(!isItemExists(title))
					{
						MediaItems m = new MediaItems(title,Integer.parseInt(year));
						addMediaToJedis(m);
					}
					else 
						System.out.println("movie " + title + " is already exists");
					
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			}
		
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	



	private void addMediaToJedis(MediaItems m) {
		// TODO Auto-generated method stub
		Map<String,String> movie  = new HashMap<String, String>();		
		movie.put("title", m.getTitle());
		movie.put("year", ""+m.getProdYear());
		Jedis jedis = new Jedis(redis_url);
		jedis.hmset(itemsTable+m.getTitle(), movie);
		jedis.close();
	}



	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
		
		Jedis jedis = new Jedis(redis_url);
		Set<String> items = jedis.keys(itemsTable+"*");
		MediaItems[] result = new MediaItems[items.size()];
		int i = 0;
		for (String string : items) {
			if (i==topN)
				break;
			Map<String,String> itemMap = jedis.hgetAll(string);
			MediaItems item = new MediaItems(itemMap.get("title"), Integer.parseInt(itemMap.get("year")));
			System.out.println(item);
			result[i++] = item;
		}
		jedis.close();
		return result;
	
	}
		

}
