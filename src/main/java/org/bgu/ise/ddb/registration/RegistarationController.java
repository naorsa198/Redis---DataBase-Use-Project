/**
 * 
 */
package org.bgu.ise.ddb.registration;



import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
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

import redis.clients.jedis.Jedis;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{
	
	public final static String usersTable = "ProjectNaorLee#";
	public final static String redis_url = "132.72.65.45";
	
	
	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		try {
			if (isExistUser(username)) {
				System.out.println("user is already exists");
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
				return;
			}
			else {
				Map<String,String> user  = new HashMap<String, String>();		
				user.put("UserName", username);
				user.put("FirstName", firstName);
				user.put("LastName", lastName);
				user.put("Password", password);
				user.put("RegistrationDate",""+new Date().getTime());
				Jedis jedis = new Jedis(redis_url);
				jedis.hmset(usersTable+username, user);
				jedis.close();
				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	
	}
	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		Jedis jedis = new Jedis(redis_url);
		result = !(jedis.hgetAll(usersTable+username)).isEmpty();
		jedis.close();
		
		return result;
		
	}
	
	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		Jedis jedis = new Jedis(redis_url);
		Map<String, String> user = jedis.hgetAll(usersTable+username);
		jedis.close();
		if (user.isEmpty())
			result = false;
		else {
			result = username.equals(user.get("UserName")) && password.equals(user.get("Password"));
		}
		
		
		
		return result;
		
	}
	
	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		Date first_date_to_consider = new Date(new Date().getTime() - days * 24 * 60 * 60 * 1000l );
		int result = 0;
		Jedis jedis = new Jedis(redis_url);
		Set<String> users = jedis.keys(usersTable+"*");
		for (String string : users) {
			long date_s = Long.parseLong(jedis.hget(string, "RegistrationDate"));
			Date date = new Date(date_s);
			if (date.getTime() >= first_date_to_consider.getTime())
				result++;			
		}
		jedis.close();
		return result;
		
	}
	
	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		Jedis jedis = new Jedis(redis_url);
		Set<String> users = jedis.keys(usersTable+"*");
		User[] result = new User[users.size()];
		int i = 0;
		for (String string : users) {
			Map<String,String> userMap = jedis.hgetAll(string);
			User user = new User(userMap.get("UserName"), userMap.get("FirstName"), userMap.get("LastName"));
			System.out.println(user);
			result[i++] = user;
			System.out.print(user);

		}
		jedis.close();
		
		return result;
	}

	
}
	
