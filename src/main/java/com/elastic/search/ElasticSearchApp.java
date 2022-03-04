package com.elastic.search;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchApp 
{
    public static void main( String[] args ) throws UnknownHostException{
    	Settings settings = Settings.builder()
    	        .put("cluster.name", "elasticsearch").build();
    	
    	PreBuiltTransportClient transport = new PreBuiltTransportClient(settings);
    	TransportClient client = transport.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
    	
    	List<DiscoveryNode> discoveryNodes = client.listedNodes();
    	for(DiscoveryNode node: discoveryNodes) {
    		System.out.println("Node: " + node);
    	}
    	
    	
    	// indexleme (veri gönderme)
  //  	indexleme(client);
    	    	
    	//elasticsearchdeki verileri javaya response olarak çekmek
   // 	getApi(client);
    	
    	//elasticsearchdeki verileri javaya id bilmeden çekmek dizi halinde
    	searchApi(client);
    	
    	//id bilerek silmek
   // 	deleteApi(client);
    	
    	//id bilmeden silmek
 //   	deleteQueryApi(client);
    	
    	transport.close();
    }
    
    public static void indexleme(TransportClient client) {
    	Map<String,Object> json = new HashMap<>();
    	json.put("name","Apple MacBook");
    	json.put("detail", "Intel Core I5, 16GB Ram");
    	json.put("price", "5100TL");
    	
    	IndexResponse indexResponse = client.prepareIndex("product", "_doc", "3")
    		.setSource(json,XContentType.JSON)
    		.get();
    	
    	System.out.println("ID: " + indexResponse.getId());
    }
    
    
 // id bilerek veri cekmek
    public static void getApi(TransportClient client) {
    	GetResponse response = client.prepareGet("product", "_doc", "1").get();		
    	Map<String,Object> source = response.getSource();  // source verinin bulundugu kisim
    	 
    	String name = (String) source.get("name");
    	String detail = (String) source.get("detail");
    	String price = (String) source.get("price");
    	
    	System.out.println("name: " + name + " detail : " + detail + " price : " + price);
    }
    
    
 // id bilmeden veri cekmek
    public static void searchApi(TransportClient client) {
    	SearchResponse response = client.prepareSearch("product")
    			.setTypes("_doc")
    			.setQuery(QueryBuilders.matchQuery("detail", "Intel")).get();
    	
    	SearchHit[] hits = response.getHits().getHits();
    	for(SearchHit hit: hits) {
    		Map<String,Object> sourceMap = hit.getSourceAsMap();
    		System.out.println("SourceAsMap : " + sourceMap);
    	}
    }
    
    
    public static void deleteApi(TransportClient client) {
    	DeleteResponse response = client.prepareDelete("product", "_doc", "1").get();
    	System.out.println("Deleted : " + response.getId());
    }
    
    
    public static void deleteQueryApi(TransportClient client) {
    	BulkByScrollResponse response =
    			  DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
    			    .filter(QueryBuilders.matchQuery("name", "Apple")) 
    			    .source("product")                                  
    			    .get();                                             
   			 
    			System.out.println("Deleted : " + response.getDeleted());
    }
}
