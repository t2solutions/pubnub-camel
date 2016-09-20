package cl.t2solutions.camel;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;

import cl.t2solutions.camel.model.PubnubEndpointType;

/**
 * Represents a pubnub endpoint.
 */

//pubnub://pubsub:DTE?publisherKey=key&operation=PUBLISH&secretKey=sec&ssl=false
@UriEndpoint(scheme = "pubnub", title = "pubnub", syntax="tipoEndpoint:canal", consumerClass = PubnubConsumer.class, label = "pubnub")
public class PubnubEndpoint extends DefaultEndpoint {
	
	private static final Logger LOG = LoggerFactory.getLogger(PubnubEndpoint.class);
   
	private PubNub pubnub;
	private PNConfiguration pnConfiguration;
		
	public PubNub getPubnub() {
		return pubnub;
	}
	public void setPubnub(PubNub pubnub) {
		this.pubnub = pubnub;
	}
	
	/**
	 * Tipo de endpoint
	 */
	@UriPath(enums="pubsub") //pubsub,presence
	@Metadata(required="true")
	private PubnubEndpointType tipoEndpoint;
	
	/**
	 * Canal
	 */
	@UriPath
	@Metadata(required="true")
	private String canal;
	
	/**
	 * key
	 */
	@UriParam	
	private String publisherKey;
	
	/**
	 * key
	 */
	@UriParam
	private String subscriberKey;
	
	/**
	 * key
	 */
	@UriParam
	@Metadata(required="true")
	private String secretKey;
	
	/**
	 * uuid
	 */
	@UriParam
	private String uuid;	
	
    public String getPublisherKey() {
		return publisherKey;
	}

	public void setPublisherKey(String publisherKey) {
		this.publisherKey = publisherKey;
	}

	public String getSubscriberKey() {
		return subscriberKey;
	}

	public void setSubscriberKey(String subscriberKey) {
		this.subscriberKey = subscriberKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getCanal() {
		return canal;
	}
	public void setCanal(String canal) {
		this.canal = canal;
	}
	
    public PubnubEndpoint() {
    }

    
	protected void doStart() throws Exception {
		this.pubnub = (getPubnub() != null ? getPubnub() : getInstance());
		super.doStart();
	}  
    
	protected void doStop() throws Exception {
		super.doStop();
		if (this.pubnub != null) {
			this.pubnub.stop();
			this.pubnub = null;
		}
	}
	
	/**
	 * obtiene instancia de Pubnub, si no existe, se crea
	 * @return (PubNub) objeto instancia
	 * @throws Exception
	 */
	private PubNub getInstance() throws Exception
	  {
		 	LOG.debug("**** VALORES PRESETEADOS *****");
		 	LOG.debug("this.subscriberKey => "+getSubscriberKey());
		 	LOG.debug("this.publisherKey =>"+getPublisherKey());
		 	LOG.debug("this.canal =>"+this.canal);
		 	LOG.debug("**** VALORES PRESETEADOS *****");
	    	
	        pnConfiguration = new PNConfiguration();	        
	    	pnConfiguration.setSecretKey(this.getSecretKey());
	    	
	    	//Verificacion del canal
	    	if (getCanal() == null)
	    		throw new Exception("Se debe ingresar el canal");
	    	
	        //Deben ir los dos
	    	if (getSubscriberKey() == null & getPublisherKey() == null)
	    		throw new Exception("Se deben ingresar los keys para publicador y suscriptor");
	    	
	    	pnConfiguration.setSubscribeKey(this.getSubscriberKey());
	    	pnConfiguration.setPublishKey(this.getPublisherKey());
	    	
	    	return new PubNub(pnConfiguration);

	  }	
	    
    public PubnubEndpoint(String uri, PubnubComponent component, PubnubEndpointType tipoEndpoint) throws Exception {
        super(uri, component);
    }
    
    @SuppressWarnings("deprecation")
	public PubnubEndpoint(String endpointUri) throws Exception {
        super(endpointUri);
    }    
    
    
    public Producer createProducer() throws Exception {
        return new PubnubProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return new PubnubConsumer(this, processor);
    }

    public boolean isSingleton() {
        return true;
    }

}
