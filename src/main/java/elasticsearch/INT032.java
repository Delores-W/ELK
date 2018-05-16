package elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class INT032 {

    public final static String HOST = "10.140.8.212";
    public final static int PORT = 9300;
    public final static String START = "2018-05-06T15:59:59.000Z";
    public final static String END = "2018-05-07T15:59:59.000Z";

    TransportClient client;

    /**
     * newgen_balance-alerts-service
     * 
     */
    private void init() throws UnknownHostException{
        // on startup
//    	TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2")); 
//    	Settings settings = Settings.builder().put("cluster.name", "name").build();
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName(INT032.HOST),INT032.PORT));
        System.out.println("Elasticsearch connect info: " + client.nodeName());
    }
    
    private void close() {
        // on shutdown
        client.close();
    }

    public static void main(String[] args) throws IOException {
    	
        INT032 es = new INT032();
        es.init();
        
        // CEST +02:00 中欧夏令时
        QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INT032.START).to(INT032.END);
        
        QueryBuilder qbBalanceUpdateReceieved = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "Balance Alerts Incoming Payload:"));
        
        QueryBuilder qbBalanceUpdateSuccess = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "SFDC System API Response: HTTP Response Status: 202"));
        
        QueryBuilder qbBalanceUpdateFailed404 = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "SFDC System API Response: HTTP Response Status: 404"));

        SearchResponse response4BalanceUpdate = es.client.prepareSearch("newgen_balance-alerts-service")
        		.setTypes("log")
        		.setQuery(qbBalanceUpdateReceieved)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4BalanceUpdateSuccess = es.client.prepareSearch("newgen_balance-alerts-service")
        		.setTypes("log")
        		.setQuery(qbBalanceUpdateSuccess)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4BalanceUpdateFailed404 = es.client.prepareSearch("newgen_balance-alerts-service")
        		.setTypes("log")
        		.setQuery(qbBalanceUpdateFailed404)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchHits hits4BalanceUpdate = response4BalanceUpdate.getHits();
        SearchHits hits4BalanceUpdateSuccess = response4BalanceUpdateSuccess.getHits();
        SearchHits hits4BalanceUpdateFailed404 = response4BalanceUpdateFailed404.getHits();
        
        // shades * size
        int pageNumUpdate = (int)hits4BalanceUpdate.totalHits / (1 * 10);
        int pageNumSuccess = (int)hits4BalanceUpdate.totalHits / (1 * 10);
        int pageNum404 = (int)hits4BalanceUpdateFailed404.totalHits / (1 * 10);
        
        // Get IDs from each hit
        String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
        Pattern pattern = Pattern.compile(regx);
        Matcher match = null;
        List<String> ids = new ArrayList<String>();
        List<String> ids404 = new ArrayList<String>();
        
        
		// get 404 failed ids
		for(int i = 0; i <= pageNum404; i++) {
			
//			System.out.println("------------------Page: " + i + "   Reason 404 ---------------------");
			
			for(SearchHit hit : response4BalanceUpdateFailed404.getHits()) {
				match = pattern.matcher(hit.getSourceAsString());
				if(match.find()) {
					ids404.add(match.group(1));
//					System.out.println(match.group(1));
				}
			}
			response4BalanceUpdateFailed404 = es.client.prepareSearchScroll(response4BalanceUpdateFailed404.getScrollId()).setScroll(new TimeValue(20000)).get();
		}

        
        // get unknown failed ids
		long startTime = System.currentTimeMillis();
        for(int i = 0; i <= pageNumUpdate; i++) {
        	
//        	System.out.println("------------------Page: " + i + "   Reason Unknown ---------------------");
	        for(SearchHit hit : response4BalanceUpdate.getHits()) {
//        		System.out.println(hit.getSourceAsString());
	        	match = pattern.matcher(hit.getSourceAsString());
	        	// query log by ID
	        	if(match.find()) {
//    				System.out.println("ID: " + match.group(1));    			
//    				QueryBuilder qbID = QueryBuilders.matchPhraseQuery("message", match.group(1));
	    			
	    			// Query log by ID and is not known reason 
	    			QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(QueryBuilders.boolQuery()
	    	        				.should(QueryBuilders.matchPhraseQuery("message", "SFDC System API Response: HTTP Response Status: 202"))
	    	        				.should(QueryBuilders.matchPhraseQuery("message", "SFDC System API Response: HTTP Response Status: 404")))
	    	        		.must(qbTime);
	    			
	    			SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen*")
	    	        		.setTypes("log")
	    	        		.setQuery(qbIDUnknow)
	    	        		.get();
	    			
	    			SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
	    			// if don't match known
	    			if (hits4IDUnknown.getTotalHits() == 0)
	    				ids.add(match.group(1));  	
	    		}
	        }
	        response4BalanceUpdate = es.client.prepareSearchScroll(response4BalanceUpdate.getScrollId()).setScroll(new TimeValue(20000)).get();
        }
        
        long endTime = System.currentTimeMillis();
        
        
		File file = null;
		File file404 = null;
		FileWriter fw = null;
		Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dirName = sdf.format(now) + " " + now.getTime();
        String dirName404 = "404";
        String path = "c:/Users/wangjm@iata.org/Desktop/";
        
        file = new File(path + dirName);
        file404 = new File(path + dirName + "/" + dirName404);
        file.mkdirs();
        file404.mkdirs();
        
        // Query and Write Unknow Failed
        for (String id : ids) {
        	QueryBuilder qbIDUnknowFailed = QueryBuilders.boolQuery()
	        		.must(QueryBuilders.matchPhraseQuery("message", id))
	        		.must(qbTime);
        	// 对应的ID 日志不会很多 不用分页
        	SearchResponse response4BalanceUpdateUnknownFailed = es.client.prepareSearch("newgen*")
             		.setTypes("log")
             		.setQuery(qbIDUnknowFailed)
             		.setSize(100)
             		.addSort("@timestamp", SortOrder.DESC)
             		.get();
        	 
        	 SearchHits hits4BalanceUpdateUnknownFailed = response4BalanceUpdateUnknownFailed.getHits();
        	 
        	 file = new File(path + dirName + "/", id + ".txt");
        	 try {
				fw = new FileWriter(file);
				for(SearchHit hit : hits4BalanceUpdateUnknownFailed) {
	 				System.out.println(hit.getSourceAsString());	 				
	 				fw.write(hit.getSourceAsString());
	 				fw.write("\r\n");
	 			}  
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				fw.close();
			}
        	 
         	System.out.println("---------------------------------Unknown Failed--------------------------------------------"); 	 
        }
        
        // Query and Write 404 Failed
        for (String id : ids404) {
        	QueryBuilder qbIDFailed404 = QueryBuilders.boolQuery()
	        		.must(QueryBuilders.matchPhraseQuery("message", id))
	        		.must(qbTime);
        	
        	 SearchResponse response4BalanceUpdateFailed404ById = es.client.prepareSearch("newgen*")
             		.setTypes("log")
             		.setQuery(qbIDFailed404)
             		.setSize(10)
             		.addSort("@timestamp", SortOrder.DESC)
             		.get();
        	 
        	 SearchHits hits4BalanceUpdateFailed404ById = response4BalanceUpdateFailed404ById.getHits();
        	 
        	 file404 = new File(path + dirName + "/" + dirName404, id + ".txt");
        	 try {
				fw = new FileWriter(file404);
				for(SearchHit hit : hits4BalanceUpdateFailed404ById) {
	 				System.out.println(hit.getSourceAsString());	 				
	 				fw.write(hit.getSourceAsString());
	 				fw.write("\r\n");
	 			}  
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				fw.close();
			}
        	 
         	System.out.println("---------------------------------404 Failed----------------------------------------"); 	 
        }
        
        System.out.println("Balance Update Hits Count: " + hits4BalanceUpdate.totalHits);
        System.out.println("SUCCESS Count: " + hits4BalanceUpdateSuccess.totalHits);
        System.out.println("Total Failed Count: " + (ids.size() + ids404.size()));
        System.out.println("Unknow Failed Count: " + ids.size());
        System.out.println("404 Failed Count: " + ids404.size());
        
        es.close();
        
        
        
        System.out.println("Query "+ hits4BalanceUpdate.totalHits + " Run Time: " + (endTime - startTime) / 1000 + "s");
    }
}
