package hello;

import java.io.IOException;

import org.springframework.stereotype.Component;

import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.appengine.AppEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

import reactor.core.publisher.Mono;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


@Component
public class CustomerHandler {
	  private static final byte[] TABLE_NAME = Bytes.toBytes("Customer-Bigtable");
	  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
	  private static final byte[] COLUMN_NAME = Bytes.toBytes("Customer");
	  private static final String secJSON="{\n" + 
	  		"  \"client_id\": \"764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com\",\n" + 
	  		"  \"client_secret\": \"d-FL95Q19q7MQmFpd7hHD0Ty\",\n" + 
	  		"  \"refresh_token\": \"1/XhLOPAN1mfmLzk84_GYsN9gg0heIEyI2MVQmNaEt_OwRNYTmLaLwSDd-qZuy4HqU\",\n" + 
	  		"  \"type\": \"authorized_user\"\n" + 
	  		"}";
	  
	  
	 // The initial connection to Cloud Bigtable is an expensive operation -- We cache this Connection
	// to speed things up.  For this sample, keeping them here is a good idea, for
	// your application, you may wish to keep this somewhere else.
	private static Connection connection = null;     // The authenticated connection
	

	public Mono<ServerResponse> create(ServerRequest request) {
		System.out.println("HTTP Method--> "+request.methodName());
		//Mono<Customer> customer =  request.bodyToMono(Customer.class);
		Mono<String> string = request.bodyToMono(String.class);
		//String secJSONHeader = request.headers().header("secJSONHeader").get(0);
		
		
		return ServerResponse.ok().build(this.postToBigTable(string));
		//return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
		//	.body(BodyInserters.fromObject("{\"status\":\" success\"}"));
	}

	private Mono<Void> postToBigTable(Mono<String> customerString) {
		// Refer to table metadata names by byte array in the HBase API
	
		//load the customer data
		Mono<String> cMono = customerString.doOnNext(cust ->{
			System.out.println("Inside the lamda doOnNext: "+cust);
			try {
				ObjectMapper mapper = new ObjectMapper();
				JsonNode custRootNode = mapper.readTree(cust);
				
				String id = custRootNode.get("id").asText();
				String name = custRootNode.get("name").asText();
				String address = custRootNode.get("address").asText();
				Customer custObj = new Customer(id,name,address);
				System.out.println("custObj: "+custObj);
				
				
				 try{
					 
					 Connection conn = getConnection();
					 System.out.println("Connected to "+conn.getConfiguration().toString());
					 // The admin API lets us create, manage and delete tables
				      //Admin admin = conn.getAdmin();
				     
				      // [END connecting_to_bigtable]

				      // [START creating_a_table]
				      // Create a table with a single column family
				     // HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				      //descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));

				      //System.out.println("Create table " + descriptor.getNameAsString());
				      //admin.createTable(descriptor);
				      // [END creating_a_table]

				      // [START writing_rows]
				      // Retrieve the table we just created so we can do some reads and writes
				      Table table = conn.getTable(TableName.valueOf(TABLE_NAME));

				      String rowKey = id;
				      // Put a single row into the table. We could also pass a list of Puts to write a batch.
				      Put put = new Put(Bytes.toBytes(rowKey));
				      put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(cust));
				      table.put(put);
				      // [END writing_rows]
				      System.out.println("PUT--> record for id "+rowKey + " added");
				      
				      // [START getting_rows]
				      Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
				      System.out.print("GET --> ");
				      System.out.println(Bytes.toString(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME)));
				      
				      // [START deleting_a_table]
				      // Clean up by disabling and then deleting the table
				      //System.out.println("Delete the table");
				      //admin.disableTable(table.getName());
				      //admin.deleteTable(table.getName());
				      // [END deleting_a_table]
					 
				 }catch (IOException e) {
					      System.err.println("Exception while running: " + e.getMessage());
					      e.printStackTrace();
				 }
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
		return cMono.thenEmpty(Mono.empty());
	}

	
	/**
	 * Connect will establish the connection to Cloud Bigtable.
	 **/
	
	public static void connect() throws IOException {
		  ObjectMapper mapper = new ObjectMapper();
			//get env data
			JsonNode vcapServices;
			String projectId = null;
			String instanceId = null;
			String email = null;
			String privateKeyData = null;
			try {
				vcapServices = mapper.readTree(System.getenv("VCAP_SERVICES"));
				//String projectId = vcapServices.get("ProjectId").toString();
				JsonNode googleBigTable =  vcapServices.get("google-bigtable");
				//System.out.println("google-bigtable --> "+googleBigTable);
				
				for (JsonNode node : googleBigTable) {
					projectId = node.get("credentials").get("ProjectId").asText();
					instanceId = node.get("credentials").get("instance_id").asText();
					email = node.get("credentials").get("Email").asText();
					privateKeyData =  node.get("credentials").get("PrivateKeyData").asText();
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.err.println(e);
			}
			
			//getConnectionDetails(projectId, instanceId, mapper);
			System.out.println("projectId: "+projectId);;
			System.out.println("instance_id: "+ instanceId);
			System.out.println("Email: "+email);;
			System.out.println("PrivateKeyData Length: "+ privateKeyData.length());

	    if (projectId == null || instanceId == null ) {
	    	System.err.println("environment variables BIGTABLE_PROJECT, and BIGTABLE_INSTANCE need to be defined.");
	      return;
	    }
	    
	    //set env variable GOOGLE_APPLICATION_CREDENTIALS
	    
	    
	    
	    
	    System.out.println("Trying to connect to BigTable ....");
	    try {
	    	// Explicitly request service account credentials from the app engine standard instance.
	    	  //GoogleCredentials credentials = AppEngineCredentials.getApplicationDefault();
	    	  //System.out.println("credentials: "+credentials.toString());
	    	  Configuration config = BigtableConfiguration.configure(projectId, instanceId);
	    	  //config.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY , email);
	    	  //config.set(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY,privateKeyData);
	    	  config.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY,secJSON);
	    	  
	    	  connection = BigtableConfiguration.connect(config);
	    }
	    catch (Exception ex) {
	    	System.err.println(ex);
	    }
	  }
	  
	  public static Connection getConnection() {
		    if (connection == null) {
		      try {
		        connect();
		      } catch (IOException e) {
		    	  System.err.println(e);
		        
		      }
		    }
		    if (connection == null) {
		    	System.err.println("BigtableHelper-No Connection");
		     
		    }
		    return connection;
		}
	  
	
}