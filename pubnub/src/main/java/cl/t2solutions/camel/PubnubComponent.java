package cl.t2solutions.camel;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.t2solutions.camel.model.PubnubEndpointType;

/**
 * Represents the component that manages {@link PubnubEndpoint}.
 */
public class PubnubComponent extends UriEndpointComponent {
	
	private static final Logger LOG = LoggerFactory.getLogger(PubnubComponent.class);
    
    public PubnubComponent() {
        super(PubnubEndpoint.class);
    }

    public PubnubComponent(CamelContext context) {
        super(context, PubnubEndpoint.class);
    }

    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
    	LOG.info("Endpoint Camel para PubNub - T2Solutions.cl 2016");
    	
    	 String[] uriParts = remaining.split(":");
    	    if (uriParts.length != 2) {
    	      LOG.error("URI Invalida: " + uri + ". Debe contener el tipo de endpoint y el canal");    	    
    	      throw new IllegalArgumentException("URI Invalida: " + uri + ". Debe contener el tipo de endpoint y el canal");
    	    }    	
    	    PubnubEndpointType endpointType = PubnubEndpointType.valueOf(uriParts[0]);
    	    String canal = uriParts[1];    	
    	
    	    //Endpoint endpoint = new PubnubEndpoint(uri, this);
    	    LOG.info("Listado de parametros ingresados");
    	    for (Map.Entry entry : parameters.entrySet()) {
    	    	LOG.info(entry.getKey() + " = " + entry.getValue());
    	    }    
    	    
    	    //Verificar si vienen los parametros ("publisherKey" o "subscriberKey") y "secretKey"
    	    if ((parameters.containsKey("publisherKey")) | (parameters.containsKey("subscriberKey")) ) {
    	    	if (!(parameters.containsKey("secretKey"))) {
    	    		throw new IllegalArgumentException("URI Invalida: " + uri + ". Se debe especificar parametro secretKey");
    	    	} 
    	    } else {
    	    	 throw new IllegalArgumentException("URI Invalida: " + uri + ". Se debe especificar parametro publisherKey y/o consumerKey ");
    	    }

	    PubnubEndpoint endpoint = new PubnubEndpoint(uri, this, endpointType);
	    setProperties(endpoint, parameters);
	    endpoint.setCanal(canal);    	    
	    
        return endpoint;
    }
}
