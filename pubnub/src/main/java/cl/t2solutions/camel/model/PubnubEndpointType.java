package cl.t2solutions.camel.model;

public enum PubnubEndpointType {
	
	  pubsub("pubsub"); //, agregar presence("presence");
	  
	  private final String text;
	  
	  private PubnubEndpointType(String text)
	  {
	    this.text = text;
	  }
	  
	  public String toString()
	  {
	    return this.text;
	  }
}


