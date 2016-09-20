package cl.t2solutions.camel;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pubnub.api.PubNubException;
import com.pubnub.api.callbacks.PNCallback;
import com.pubnub.api.models.consumer.PNPublishResult;
import com.pubnub.api.models.consumer.PNStatus;

/**
 * Productor de mensaje hacia Pubnub
 */
public class PubnubProducer extends DefaultProducer {
    private static final Logger LOG = LoggerFactory.getLogger(PubnubProducer.class);
    private PubnubEndpoint endpoint;
    
    private Exchange localExchange;
    
    private String mensajeProducir = "";

    public PubnubProducer(PubnubEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    public void process(Exchange exchange) throws Exception {
    	LOG.debug("Aqui se procesa como productor");
    	localExchange = exchange;    	
    	mensajeProducir = exchange.getIn().getMandatoryBody(String.class);
    	LOG.debug("Mensaje ->"+mensajeProducir);
    	
    	//PNPublishResult resultado = 
    	endpoint.getPubnub().publish()
	        .message(mensajeProducir) //Arrays.asList("hello", "there")
	        .channel(endpoint.getCanal())
	        .usePOST(true)
	        .shouldStore(true)
	        .async(new PNCallback<PNPublishResult>() {
	            @Override
	            public void onResponse(PNPublishResult result, PNStatus status) {
	            			LOG.debug("Ha ocurrido un problema al publicar el mensaje...");
	            			LOG.debug("resultado es error -> "+ status.isError());
							localExchange.getIn().setHeader("CamelPubnubUUID",
									endpoint.getUuid());
							localExchange.getIn().setHeader("CamelPubnubChannel",
									endpoint.getCanal());
							if (status.isError()) {
								PubNubException pex = (PubNubException) status
										.getErrorData().getThrowable();
	
								LOG.debug("cod error ->"+ pex.getPubnubError().getErrorCode());
								LOG.debug("cod error extendido ->"+ pex.getPubnubError().getErrorCodeExtended());
								LOG.debug("mensaje visible hacia afuera ->"+ pex.getPubnubError().getMessage());
								LOG.debug("mensaje de error detallado -> "+ pex.getErrormsg());
	
								LOG.debug("client request -> "+ status.getClientRequest());
							} else {
								PubnubProducer.this.manejarNoFueError();
							}
	            }

	        });	
    	
    	exchange = localExchange;
    }

    
	private void manejarNoFueError() {
		if (localExchange.getPattern().isOutCapable()) {
			localExchange.getOut().copyFrom(localExchange.getIn());
			localExchange.getOut().setBody(mensajeProducir);				
		}
	}
    
    
}
