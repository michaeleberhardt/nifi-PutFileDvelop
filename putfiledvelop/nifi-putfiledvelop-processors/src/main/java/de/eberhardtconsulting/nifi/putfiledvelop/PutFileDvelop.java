/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.eberhardtconsulting.nifi.putfiledvelop;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
@Tags({"PutFileDvelop"})
@CapabilityDescription("This Processor uploads Files to d.velop documents. The content of the FlowFile is considered as the file, properties prefixed with dv_ are mapped to d.velop document attributes.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="dv_*", description="Attributes to be mapped to documents")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PutFileDvelop extends AbstractProcessor {
	private ComponentLog log;
	
    public static final PropertyDescriptor PROP_API_KEY = new PropertyDescriptor
            .Builder().name("API_KEY")
            .displayName("API Key")
            .description("API Key to access tenant")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    
    public static final PropertyDescriptor PROP_BASE_URI = new PropertyDescriptor
            .Builder().name("BASE_URI")
            .displayName("Base URI")
            .description("Base URI of tenant")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
  
    public static final PropertyDescriptor PROP_SOURCE_CATEGORY = new PropertyDescriptor
            .Builder().name("SOURCE_CATEGORY")
            .displayName("Source Category")
            .description("ID of Source Category")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();    
    
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyName) {
        return new PropertyDescriptor.Builder()
            .name(propertyName)
            .dynamic(true)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    }

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed processing")
            .build();
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Succeed processing")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
    	log = getLogger();
    	log.info("PutDvelopFile - INIT"); 
        descriptors = new ArrayList<>();
        descriptors.add(PROP_API_KEY);
        descriptors.add(PROP_BASE_URI);
        descriptors.add(PROP_SOURCE_CATEGORY);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
    	log.info("PutDvelopFile - ONTRIGGER"); 
    	FlowFile flowFile = session.get();
    	var apiKey = context.getProperty(PROP_API_KEY).evaluateAttributeExpressions(flowFile).getValue(); 
    	var baseUri = context.getProperty(PROP_BASE_URI).evaluateAttributeExpressions(flowFile).getValue();        
        DvCloudConnector dv = new DvCloudConnector(apiKey, baseUri, session, flowFile, log, context);
        if(!dv.Init()) {
        	log.error("Error initializing DvCloudConnector - check API-Key and Base-URI");
        	return;
        }
        
        try {
        	if(dv.UploadFile()) {
        		log.info("Upload successful!");
        		session.transfer(flowFile, REL_SUCCESS);   
        	}
        	else {
        		log.error("Upload failed!");
        		session.transfer(flowFile, REL_FAILURE);   
        	}
        }
        catch(Exception ex) {
        	log.error(ex.toString() + ex.getMessage());
        	session.transfer(flowFile, REL_FAILURE);  
        }
        // TODO implement
    }
    

}
