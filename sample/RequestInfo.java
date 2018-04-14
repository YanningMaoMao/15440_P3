import java.io.Serializable;

/**
 * A class that represents a request. The class stores information about the request,
 * and has methods that can check information about the request.
 * 
 * @author YanningMao <yanningm@andrew.cmu.edu>
 *
 */
public class RequestInfo implements Serializable {
	
	
	/* ------------------   constants   ------------------ */
	
	
	// serialization version ID
	private static final long serialVersionUID = -299850267299447367L;
	
	// maximum milliseconds a browse request can be in queue
	private static final long BROWSE_MAX_WAIT_MILLIS = 800;
	// maximum milliseconds a purchase request can be in queue
	private static final long PURCHASE_MAX_WAIT_MILLIS = 1800;
	

	/* ------------------   instance variables   ------------------ */
	
	
	// the request itself
	private Cloud.FrontEndOps.Request r;
	// the time when the request was put in queue
	private long startTime;

	
	/* ------------------   constructor   ------------------ */

	
	public RequestInfo(Cloud.FrontEndOps.Request r, long startTime) {
		this.r = r;
		this.startTime = startTime;
	}
	
	
	/* ------------------   instance methods   ------------------ */

	
	/**
	 * Gets the request.
	 * 
	 * @return r, the request itself.
	 */
	public Cloud.FrontEndOps.Request getRequest() {
		return r;
	}
	
	/**
	 * Checks if the request is a purchase request.
	 * 
	 * @return true, if the request is a purchase request;
	 * 	   false, otherwise
	 */
	public boolean isPurchase() {
		return r.isPurchase;
	}
	
	/**
	 * Gets request ID.
	 * 
	 * @return id, the ID of the request.
	 */
	public int getID() {
		return r.id;
	}
	
	/**
	 * Determines whether the request can be dropped from the queue.
	 * A request will be dropped if it will largely timeout.
	 * 
	 * @return true, if the request can be dropped;
	 * 	   false, otherwise
	 */
	public boolean canDrop() {
		
		// calculate the time since the request has been in queue
		long currTime = System.currentTimeMillis();
		long waitedTime = currTime - startTime;
		
		// checks if the request will largely timeout
		if (r.isPurchase) {
			return (waitedTime > PURCHASE_MAX_WAIT_MILLIS);
		}
		else {
			return (waitedTime > BROWSE_MAX_WAIT_MILLIS);
		}
	}
	
	
}


