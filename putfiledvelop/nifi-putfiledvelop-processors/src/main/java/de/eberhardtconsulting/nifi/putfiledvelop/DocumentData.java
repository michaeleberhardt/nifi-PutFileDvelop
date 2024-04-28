package de.eberhardtconsulting.nifi.putfiledvelop;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;

public class DocumentData {
    private String filename;
    private String sourceCategory;
    private String sourceId;
    private String contentLocationUri;
    private SourceProperties sourceProperties;

    public DocumentData() {}
    
    public DocumentData(FlowFile flowFile, String repoId, ProcessContext context, ComponentLog log)
    {
    	this.filename = context.getProperty("filename").evaluateAttributeExpressions(flowFile).getValue();  	
    	this.sourceId = "/dms/r/" + repoId + "/source";
    	this.sourceCategory = context.getProperty("SOURCE_CATEGORY").evaluateAttributeExpressions(flowFile).getValue(); 
    	this.contentLocationUri = context.getProperty("dv_contentLocationUri").evaluateAttributeExpressions(flowFile).getValue(); 
    	
    	this.sourceProperties = new SourceProperties();
    	this.sourceProperties.properties = new ArrayList<SourceProperties.Property>();
    	
    	
    	Map<String, String> attributeMap = context.getAllProperties();
        for (String key : attributeMap.keySet()) {
        	log.info("DocumentData KEY: " + key + ", VALUE: " + context.getProperty(key).evaluateAttributeExpressions(flowFile).getValue());
        	if(key.startsWith("dv_"))
         	{
        		SourceProperties.Property p = new SourceProperties.Property();
        		p.values = new ArrayList<String>();
        		p.key = key.replace("dv_","");
        		p.values.add(context.getProperty(key).evaluateAttributeExpressions(flowFile).getValue());
        		this.sourceProperties.properties.add(p);
         	}
        }
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getSourceCategory() {
        return sourceCategory;
    }

    public void setSourceCategory(String sourceCategory) {
        this.sourceCategory = sourceCategory;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getContentLocationUri() {
        return contentLocationUri;
    }

    public void setContentLocationUri(String contentLocationUri) {
        this.contentLocationUri = contentLocationUri;
    }

    public SourceProperties getSourceProperties() {
        return sourceProperties;
    }

    public void setSourceProperties(SourceProperties sourceProperties) {
        this.sourceProperties = sourceProperties;
    }

    public static class SourceProperties {
        private List<Property> properties;

        public List<Property> getProperties() {
            return properties;
        }

        public void setProperties(List<Property> properties) {
            this.properties = properties;
        }

        public static class Property {
            private String key;
            private List<String> values;

            public String getKey() {
                return key;
            }

            public void setKey(String key) {
                this.key = key;
            }

            public List<String> getValues() {
                return values;
            }

            public void setValues(List<String> values) {
                this.values = values;
            }
        }
    }
}