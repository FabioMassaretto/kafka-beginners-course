package kafka.exemplebymyself;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    private Client hosebirdClient;
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

    public TwitterProducer() {
        settingUpTwitter();
    }

    public void settingUpTwitter(){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,
                consumerSecret,
                token,
                tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(this.msgQueue))
                .eventMessageQueue(this.eventQueue);                          // optional: use this if you want to process client events

        this.hosebirdClient = builder.build();
        // Attempts to establish a connection.
        hosebirdClient.connect();


    }

    public List<String> getTweets(){
        List<String> listTweets = new ArrayList<>();
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = this.msgQueue.take();
                listTweets.add(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                hosebirdClient.stop();
            }
            //something(msg);
            //profit();
        }
        return listTweets;
    }
}
