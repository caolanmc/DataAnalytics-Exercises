import java.net.*;
import java.io.*;

/**
 * A socket-based server of an infinite stream of random twoots (for Assignment 5)
 */
public class Twotter { 
	
	static int port = 9999;
	
	static String[] subjects = { "@elon", "@jeff" /*optional: repeat words to see how this influences the hashtag distribution, e.g., you could add another "@elon" to this array*/ };
	
	static String[] predicates = { "sells", "likes", "hates", "buys" };
	
	static String[] objects1 = { "#bitcoin", "#rockets", "#dogecoin", "@jeff", "@elon" } ;
	
	//static String[] objects = Stream.of(objects1, subjects).flatMap(Stream::of).toArray(String[]::new);
	
	static String randomSentence() {

		   String subject = subjects[(int) (Math.random() * subjects.length)];
		   String predicate = predicates[(int) (Math.random() * predicates.length)];
		   String object = objects1[(int) (Math.random() * objects1.length)];
		   
		   return subject + " " + predicate + " " + object;
		
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		System.out.println("Twotter server waiting for connection at port " + port + "...");

		try {
			
			ServerSocket s = new ServerSocket(port);
			
			Socket cs = s.accept();
			
			System.out.println("Client connected. Serving twoots...");
			
			PrintWriter pw = new PrintWriter(cs.getOutputStream());
			
			for(;;) {
				
				String twoot = randomSentence();
				 			
				System.out.println("Emitting " + twoot);
				
				pw.println(twoot);
				
				pw.flush();				
				
			}			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 		

	}

}
