package com.data.new1;


import java.io.IOException;
import java.io.PrintWriter;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterException;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.StatusListener;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;



public class Code {

	private class MahoutListener implements StatusListener {
    	long count = 0;
    	static final long maxCount = 10; // change this if you want another number of tweets to collect
    	PrintWriter out;
    	TwitterStream tweetStream;
    	
    	MahoutListener (TwitterStream ts) throws IOException {
    		tweetStream = ts;
    		
    		//out = new PrintWriter(new BufferedWriter(new FileWriter("tweets.txt")));
    	}
    	
        public void onStatus(Status status) {
        	String	username = status.getUser().getScreenName();
        	String text = status.getText().replace('\n', ' ');
        	int friends = status.getUser().getFriendsCount();
            //System.out.println(username + "\t" + text + "\t" + friends);
            System.out.println("1");
            System.out.println("2");
            System.out.println("3");
            Cluster cluster;
    		Session session;
    		cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
    		session = cluster.connect("mani");
    		if(count <= maxCount)
    		{
    		session.execute("INSERT INTO twitter(count, username, text)VALUES("+friends+",'"+username+"','"+text+"')");
    		ResultSet rs=session.execute("select * from twitter");
    		for(Row row : rs)
    		{
    			System.out.format("%d %s %s\n", row.getInt("count"), row.getString("username"), row.getString("text"));
    		}
    		
            //out.println(username + "\t" + text);
            count++;
            }
    		else
    		{
    			tweetStream.shutdown();
            	cluster.close();
    		}
        }

        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        }

        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        }

        public void onScrubGeo(long userId, long upToStatusId) {
        }

        public void onException(Exception ex) {
            ex.printStackTrace();
        }

		public void onStallWarning(StallWarning arg0) {
			// TODO Auto-generated method stub
			
		}
	}
	
	StatusListener makeListener(TwitterStream ts) throws IOException {
		return this.new MahoutListener(ts);
}
	
	
	
	
	
	
	
	public static void main(String[] args) throws TwitterException, IOException{
		
		
		
		ConfigurationBuilder df = new ConfigurationBuilder();
		
		df.setDebugEnabled(true)
				
				.setOAuthConsumerKey("e8bDTrZBG2fJLiP5r85QMTdNC")
				.setOAuthConsumerSecret("9uEOb8Rw02YjiRLr6n4JgzYYH03OcEXMCmDL12rttbiv4Bhfgp")
				.setOAuthAccessToken("344053333-OeRpSBFwMWMtxHztMa0GRKE77pDBTCLrelZDGc7G")
				.setOAuthAccessTokenSecret("YGzOSgTySsWZbhK0btYhR3PYUVYWVKL98LV5fp6LFqVlf");

	
		TwitterStream twitterStream = new TwitterStreamFactory(df.build()).getInstance();
        Code td = new Code();
        StatusListener listener = td.makeListener(twitterStream);
        twitterStream.addListener(listener);
        twitterStream.sample();
        
	}
	
		
}

