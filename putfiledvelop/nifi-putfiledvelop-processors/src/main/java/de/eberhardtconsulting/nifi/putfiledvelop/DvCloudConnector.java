package de.eberhardtconsulting.nifi.putfiledvelop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class DvCloudConnector {
	
	private String baseUri;
	private String repoId;
	private String auth;
	private ProcessSession session;
	private FlowFile flowFile;
	private ComponentLog log;
	private ProcessContext context;
	
	public DvCloudConnector(String apiKey, String baseUri, ProcessSession session, FlowFile flowFile, ComponentLog log, ProcessContext context)
	{
		this.baseUri = baseUri;
		this.repoId = "";
		this.auth = "Bearer " + apiKey;
		this.session = session;
		this.flowFile = flowFile;
		this.log = log;
		this.context = context;
	}
	
	public boolean Init()
	{
		log.info("DvCloudConnector INIT");
		if(this.GetRepoId()) return true;
		return false;
	}
	
	public boolean UploadFile() throws URISyntaxException
	{
		log.info("UploadFile BEGIN");
		try {
			
			final var byteArrayOutputStream = new ByteArrayOutputStream();	
			this.session.exportTo(this.flowFile, byteArrayOutputStream);
			DocumentData docData = new DocumentData(this.flowFile, this.repoId, this.context, this.log);
			String byteData = byteArrayOutputStream.toString();
			log.info("UploadFile byteData length: " + byteData.length());
			docData.setContentLocationUri(this.uploadChunk(docData.getSourceCategory(),byteArrayOutputStream.toByteArray()));
			if(this.saveDoc(docData)) {
				log.info("UploadFile TRUE");
				return true;
			}
			return false;
		}
		catch(Exception ex)
		{
			log.error("UploadFile: " + ex.getMessage());
			ex.printStackTrace();
		}
		log.info("UploadFile FALSE");
		return false;
	}
	
	private boolean GetRepoId()
	{
		log.info("GetRepoId BEGIN");
		String uri = this.baseUri + "/dms/r";
		System.out.println(uri);
		System.out.println(auth);
		HttpClient client = HttpClient.newHttpClient();
	    HttpRequest request = HttpRequest.newBuilder()
	          .uri(URI.create(uri))
	          .header("Authorization", auth)
	          .header("Accept", "*/*")
	          .header("Accept", "application/hal+json")
	          .build();

	    HttpResponse<String> response;
		try {
			ObjectMapper om = new ObjectMapper();
			response = client.send(request, BodyHandlers.ofString());			
			if(response.statusCode() != 200) return false;		
			JsonNode node = om.readTree(response.body());
			ArrayNode arnode = (ArrayNode) node.at("/repositories");
			this.repoId = arnode.get(0).get("id").asText();
			log.info("GetRepoId: " + this.repoId);
			if(this.repoId.length() > 0 ) return true;
		} 
		catch (IOException e) 
		{
			log.error("GetRepoId: " + e.getMessage());
			e.printStackTrace();
		} 
		catch (InterruptedException e) 
		{
			log.error("GetRepoId: " + e.getMessage());
			e.printStackTrace();
		}    
		return false;
	}

	public String uploadChunk(String sourceCategoryId, byte[] buf) throws URISyntaxException {		  
		String uri = this.baseUri + "/dms/r/" + this.repoId + "/blob/chunk/";
		log.info("uploadChunk BEGIN");
		log.info("uploadChunk - Buffer length: " + buf.length);
		try
		{
			HttpClient client = HttpClient.newHttpClient();
			HttpRequest request = HttpRequest
			  .newBuilder()
			  .uri(URI.create(uri))
			  .POST(HttpRequest.BodyPublishers.ofByteArray(buf))
			  //.POST(HttpRequest.BodyPublishers.ofFile(Paths.get("c:\\temp\\38672.pdf")))
			  .header("Accept", "*/*")
			  .header("Authorization", auth)
			  .header("Content-Type","text/plain")
			  .build();
			log.info("uploadChunk - Posting..");
			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			log.info("uploadChungk - Body: "+ response.body());
			log.info("uploadChunk - StatusCode: "+ response.statusCode());
	
			log.info("uploadChunk - Location: "+ response.headers().firstValue("location").get());
			log.info("uploadChunk END");
			return response.headers().firstValue("location").get();
		} 
		catch (IOException | InterruptedException e) {
				log.error("uploadChung: " + e.getMessage());
			   e.printStackTrace();
		}	
		return "";
	}
	
	public boolean saveDoc(DocumentData docData)
	{
		log.info("saveDoc - BEGIN");
		String uri = this.baseUri + "/dms/r/" + this.repoId + "/o2m";
		log.info("saveDoc - URI: " + uri);
		
		
		try
		{
			log.info("saveDoc - JSON: " + new ObjectMapper().writeValueAsString(docData));
			HttpClient client = HttpClient.newHttpClient();
			HttpRequest request = HttpRequest
			  .newBuilder()
			  .uri(URI.create(uri))
			  .POST(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(docData)))
			  .header("Accept", "*/*")
			  .header("Authorization", auth)
			  .header("Content-Type","text/plain")
			  .header("Origin",this.baseUri)
			  .build();
			log.info("saveDoc - Posting..");
			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			
			log.info("saveDoc - Body: " + response.body());
			log.info("saveDoc - StatusCode: " + response.statusCode());
			log.info("saveDoc - Headers: " + response.headers());
			log.info("saveDoc - END");
			if(response.statusCode() != 200 && response.statusCode() != 201)
			{
				log.error("saveDoc - ERROR: " + response.headers());
				return false;
			}
			return true;
		} 
		catch (IOException | InterruptedException e) {
			   e.printStackTrace();
			   log.error("saveDoc: " + e.getMessage());
			   return false;
		}	
	}
	


}
