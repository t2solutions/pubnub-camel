package cl.t2solutions.camel;

import java.util.Arrays;
import java.util.Date;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
//import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.PubNubException;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNLogVerbosity;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;

/**
 * Consumidor de mensaje desde Pubnub.
 */
public class PubnubConsumer extends DefaultConsumer { //ScheduledPollConsumer
	
	private static final Logger LOG = LoggerFactory.getLogger(PubnubConsumer.class);
    
	private final PubnubEndpoint endpoint;
    
    private PNConfiguration pnConfiguration;
    
    public PNConfiguration getPnConfiguration() {
		return pnConfiguration;
	}
    public void setPnConfiguration(PNConfiguration pnConfiguration) {
		this.pnConfiguration = pnConfiguration;
	}
    
    public PubnubConsumer(PubnubEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }
    

    @Override
	protected void doStart() throws Exception {
    	LOG.info("Iniciando endpoint como consumidor");
		super.doStart();
		PNConfiguration pnConfig = endpoint.getPubnub().getConfiguration();
		pnConfig.setLogVerbosity(PNLogVerbosity.BODY);
		endpoint.getPubnub().addListener(getSubscribeCallback());
		this.setPnConfiguration(pnConfig);
		conectarHaciaPubnub();
	}

    @Override
	protected void doResume() throws Exception {
		super.doResume();
		PNConfiguration pnConfig = endpoint.getPubnub().getConfiguration();
		pnConfig.setLogVerbosity(PNLogVerbosity.BODY);
		endpoint.getPubnub().addListener(getSubscribeCallback());
		conectarHaciaPubnub();
	}

    @Override
	protected void doStop() throws Exception {
    	endpoint.getPubnub().removeListener(getSubscribeCallback());
		desconectarPubnub();
		super.doStop();
	}

    @Override
	protected void doSuspend() throws Exception {
    	endpoint.getPubnub().removeListener(getSubscribeCallback());
    	desconectarPubnub();
		super.doSuspend();
	}   
    
    /**
     * Suscribirse a los canales definidos
     * @throws Exception
     */
    private void conectarHaciaPubnub() throws Exception {
    	LOG.info("Intentando conectar a Pubnub...");
    	LOG.info("Canal: "+endpoint.getCanal());
    	endpoint.getPubnub().subscribe()
		    .channels(Arrays.asList(endpoint.getCanal())) // 
		    .execute();	
    }
    
    /**
     * Desuscribirse de los canales definidos
     * @throws Exception
     */
    private void desconectarPubnub() throws Exception {
    	LOG.info("Desconectando de Pubnub...");
    	LOG.info("Canal: "+endpoint.getCanal());
    	endpoint.getPubnub().unsubscribe()
	    	.channels(Arrays.asList(endpoint.getCanal())) // 
	    	.execute();    	
    }

    protected SubscribeCallback subscribeCallback = new SubscribeCallback() {
		
	    @Override
	    public void status(PubNub pubnub, PNStatus status) {
	    	if (status.isError()) {
				PubNubException pex = (PubNubException) status
						.getErrorData().getThrowable();
				LOG.error("Ha ocurrido un problema al consumir mensajes...");
				LOG.debug("cod error ->"+ pex.getPubnubError().getErrorCode());
				LOG.debug("cod error extendido ->"+ pex.getPubnubError().getErrorCodeExtended());
				LOG.debug("mensaje visible hacia afuera ->"+ pex.getPubnubError().getMessage());
				LOG.debug("mensaje de error detallado -> "+ pex.getErrormsg());
				LOG.debug("Request de cliente -> "+ status.getClientRequest());
	    	}
	    	
	    }
	 
	    @Override
	    public void message(PubNub pubnub, PNMessageResult message) {
	    	
	    	LOG.debug("***** Ha entrado un mensaje *****");
	    	//JsonNode message.getMessage()
	    	JsonNode msg = message.getMessage();
	    	LOG.debug("Contenido ->"+msg.asText());
	    	LOG.debug("Canal suscrito ->"+message.getSubscribedChannel());
	    	LOG.debug("Canal actual ->"+message.getActualChannel());
	    	LOG.debug("Metadata mensaje -> "+message.getUserMetadata());
	    	LOG.debug("***** Fin del mensaje *****\n");
	    	
	    	Exchange exchange = endpoint.createExchange(); 
	    	
	    	Message mssg = exchange.getIn();
	    	exchange.getIn().setBody(msg); //msg.asText() usar JsonNode
	    	mssg.setHeader("CamelPubNubTimeToken", message.getTimetoken());
	        mssg.setHeader("CamelPubNubChannel", message.getSubscribedChannel());
	        
	        try {
	        	getProcessor().process(exchange);
	        } catch (Exception e) {
	          exchange.setException(e);
	          getExceptionHandler().handleException("Error procesando exchange -> ", exchange, e);
	        }	    	
	        
	    }
	 
	    @Override
	    public void presence(PubNub pubnub, PNPresenceEventResult presence) {
	    	//TODO: No implementado Presence por ahora
	    }
	};
	
	/**
	 * Devuelve el subscribe callback
	 * @return
	 */
	public SubscribeCallback getSubscribeCallback() {
		return subscribeCallback;
	}
    

}
