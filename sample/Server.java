import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * This class is a server that works in a scaling system for request processing.
 * The system contains three tier - the front tier, the middle tier, and the cache tier.
 * An instance of this class can work as any of the three roles, and the server instances
 * cooperate together to improve the performance of the request processing.
 * 
 * @author YanningMao <yanningm@andrew.cmu.edu>
 *
 */

public class Server extends UnicastRemoteObject implements ServerInterf, Cloud.DatabaseOps {
	
	/* -------------------------   constants   ------------------------- */
	
	// serialization version ID
	private static final long serialVersionUID = -1960888810093318280L;
	
	// the rate for determining number of servers to scale out
	private static final double REQUEST_MID_SERVER_RATE = 1.5;
	
	// the maximum number of requests in the central queue
	private static final int MAX_UNPROCESSED_REQUESTS = 1024;

	// the virtual memory ID of the master server
	public static final int MASTER_SERVER_VMID = 1;
	
	// the time interval for scale out
	private static final int SCALE_OUT_INTERVAL = 1500;
	// the time interval for scale in
	private static final int SCALE_IN_INTERVAL = 500;
	
	// the maximum number of continuous slow request arrivals
	private static final int MAX_SLOW_ARRIVALS = 2;
	// the maximum time between two request arrivals
	private static final int MAX_FRONT_ARRIVAL_INTERVAL = 1000;
	private static final int MAX_MID_ARRIVAL_INTERVAL = 2000;
	// the time between two request arrivals viewed as slow
	private static final int SLOW_FRONT_ARRIVAL_INTERVAL = 500;
	private static final int SLOW_MID_ARRIVAL_INTERVAL = 700;
	
	// minimum number of servers
	private static final int MIN_FRONT_TIER_SERVERS = 1;
	private static final int MIN_MID_TIER_SERVERS = 1;

	// virtual memory ID of the server
	private static int vmID;
	// cloud IP used to connect with the Cloud
	private static String cloudIP;
	// cloud port used to connect with the Cloud
	private static int cloudPort;
	
	// the role of the server
	private static ServerRole role;
	
	// the RMI registry
	private static Registry registry;
	
	// the ServerLib object used by the server to communicate with the Cloud
	private static ServerLib SL;
	
	// the object of the server itself
	private static ServerInterf selfServer;
	// the object of the master server
	private static ServerInterf masterServer;
	// the object of the database server
	private static Cloud.DatabaseOps databaseServer;
	
	// initial number of front-tier and middle-tier servers
	private static int initFrontTierServers;
	private static int initMiddleTierServers;
	
	// stores the roles of the servers
	private static ConcurrentMap<Integer, ServerRole> serverRoles = new ConcurrentHashMap<>();
	private static ConcurrentMap<Integer, ServerRole> frontTierServers = new ConcurrentHashMap<>();
	private static ConcurrentMap<Integer, ServerRole> midTierServers = new ConcurrentHashMap<>();
	private static ConcurrentMap<Integer, ServerRole> cacheTierServers = new ConcurrentHashMap<>();
	
	// stores all the requests on the master server
	private static BlockingQueue<RequestInfo> purchaseRequests = new ArrayBlockingQueue<>(MAX_UNPROCESSED_REQUESTS, true);
	private static BlockingQueue<RequestInfo> browseRequests = new ArrayBlockingQueue<>(MAX_UNPROCESSED_REQUESTS, true);
	
	// the cache used by the cache server
	private static ConcurrentMap<String, String> cache = new ConcurrentHashMap<>();

	// scaling time stamps
	private static long lastScaleOutTime = System.currentTimeMillis();
	private static long lastFrontScaleInTime = System.currentTimeMillis();
	private static long lastMidScaleInTime = System.currentTimeMillis();
	
	// the last time a request arrives
	private static long lastRequestArrivalTime = System.currentTimeMillis();
	// number of continuous slow request arrivals
	private static int numContSlowArrivals = 0;
	
	/**
	 * Server constructor that inherits from its super class.
	 * @throws RemoteException
	 */
	public Server() throws RemoteException {
		super();
	}

	
	/**
	 * Scales out the number of front-tier and middle-tier servers, and drops the requests
	 * that might time out in the central queue.
	 */
	private static void scaleOut() {
		
		// initialize the number of front-tier and middle-tier servers to be added
		int newMids = 0;
		int newFronts = 0;
		
		// calculate the number of requests to be dropped
		int totalRequests = purchaseRequests.size() + browseRequests.size();
		int totalVMs = midTierServers.size() + frontTierServers.size();
		newMids = (int) Math.round(totalRequests - totalVMs * REQUEST_MID_SERVER_RATE);
		newFronts = newMids / 4;
		
		// if no need to scale out, return
		if (newMids <= 0) {
			return;
		}
		
		// start new front tier servers
		for (int i = 0; i < newFronts; i ++) {
			int slaveVMID = SL.startVM();
			serverRoles.put(slaveVMID, ServerRole.FRONT_TIER);
			frontTierServers.put(slaveVMID, ServerRole.FRONT_TIER);
		}
		
		// start new middle tier servers
		for (int i = 0; i < newMids; i ++) {
			int slaveVMID = SL.startVM();
			serverRoles.put(slaveVMID, ServerRole.MID_TIER);
			midTierServers.put(slaveVMID, ServerRole.MID_TIER);
		}

		// drop possible timeout browse requests
		synchronized (browseRequests) {
			RequestInfo r = browseRequests.peek();
			// check if timeout
			while (r != null) {
				// drop if timeout
				if (r.canDrop()) {
					// System.out.println("Drop request : " + r.getRequest());
					browseRequests.poll();
					SL.drop(r.getRequest());
				}
				// stop dropping if earliest request does not timeout
				else {
					return;
				}
				// check next request
				r = browseRequests.peek();
			}
		}

		// drop possible timeout purchase requests
		synchronized (purchaseRequests) {
			RequestInfo r = purchaseRequests.peek();
			// check if timeout
			while (r != null) {
				// drop if timeout
				if (r.canDrop()) {
					// System.out.println("Drop request : " + r.getRequest());
					purchaseRequests.poll();
					SL.drop(r.getRequest());
				}
				// stop dropping if earliest request does not timeout
				else {
					return;
				}
				// check next request
				r = purchaseRequests.peek();
			}
		}

	}

	
	@Override
	public int getVMID() {
		return vmID;
	}
	
	@Override
	public boolean isMasterServer() {
		return vmID == 1;
	}
	
	@Override
	public ServerRole assignServerRole(int id) throws RemoteException {
		return serverRoles.get(id);
	}
	
	@Override
	public synchronized boolean terminateServer(int vmID) {
				
		// master can never terminate
		if (vmID == MASTER_SERVER_VMID) {
			return false;
		}
		
		// get the role of the slave server
		if (frontTierServers.containsKey(vmID)) {
			return stopFrontTierSlaveServer(vmID);
		}
		else if (midTierServers.containsKey(vmID)) {
			return stopMidTierSlaveServer(vmID);
		}
		else if (cacheTierServers.containsKey(vmID)) {
			return stopCacheTierSlaveServer(vmID);
		}
		else {
			return false;
		}
		
	}
	
	@Override
	public synchronized void forceTerminateServer(int id) {
		ServerRole serverRole = serverRoles.remove(id);
		if (serverRole == null) {
			return;
		}
		else if (serverRole.equals(ServerRole.FRONT_TIER)) {
			frontTierServers.remove(id);
		}
		else if (serverRole.equals(ServerRole.MID_TIER)) {
			midTierServers.remove(id);
		}
		else {
			cacheTierServers.remove(id);
		}
	}
	
	@Override
	public String get(String key) throws RemoteException {
		assert(cache != null);
		
		// try to get from the cache
		String val = cache.get(key);
		// System.out.println("DB get : key = " + key + ", val = " + val);
		
		// if data is not cached
		if (val == null) {
			val = databaseServer.get(key);
			// cache the data
			cache.put(key, val);
		}
		
		// return the result
		return val;
	}

	@Override
	public boolean set(String key, String val, String auth) throws RemoteException {
		return databaseServer.set(key, val, auth);
	}

	@Override
	public boolean transaction(String item, float price, int qty) throws RemoteException {
		return databaseServer.transaction(item, price, qty);
	}
	
	@Override
	public void addRequest(RequestInfo r) {
	
		// add the request to the queue
		if (r.getRequest().isPurchase) {
			purchaseRequests.add(r);
		}
		else {
			browseRequests.add(r);
		}
		
	}
	
	@Override
	public RequestInfo getRequest() {
		RequestInfo r;
		try {
			while (true) {
				
				// get a browse request if there is no purchase request
				if (purchaseRequests.size() == 0) {
					r = browseRequests.poll(MAX_MID_ARRIVAL_INTERVAL, TimeUnit.MILLISECONDS);
				}
				// get a purchase request if there is no browse request
				else if (browseRequests.size() == 0) {
					r = purchaseRequests.poll(MAX_MID_ARRIVAL_INTERVAL, TimeUnit.MILLISECONDS);
				}
				// get a purchase request if there are more purchase requests
				else if (purchaseRequests.peek().getID() < browseRequests.peek().getID()) {
					r = purchaseRequests.poll(MAX_MID_ARRIVAL_INTERVAL, TimeUnit.MILLISECONDS);
				}
				// get a browse request if there are more browse requests
				else {
					r = browseRequests.poll(MAX_MID_ARRIVAL_INTERVAL, TimeUnit.MILLISECONDS);
				}
				
				// return null if exceed maximum waiting time
				if (r == null) {
					return r;
				}
				
				// drop if cannot be processed in time
				else if (r.canDrop()) {
					SL.drop(r.getRequest());
				}
				
				// return if can be processed in time
				else {
					return r;
				}
			}
		} catch (Exception e) {
			return null;
		}
	}
	
	/**
	 * Determines whether the front-tier server can terminate.
	 * This method is called by the slave front-tier server when it thinks it should
	 * terminate due to slow request arrival rate.
	 * The master server returns true and allows it to terminate if the slave server
	 * is not a master server, and its termination request is not too close to the
	 * last slave termination.
	 * The master server returns false and refuses it to terminate if the server
	 * is master server, or its termination request is too close to the last slave
	 * termination.
	 * 
	 * @param vmID, the virtual memory ID of the server asking for termination
	 * @return true, if the master approves the termination; false, otherwise
	 */
	private boolean stopFrontTierSlaveServer(int vmID) {
		
		// cannot have less than minimum number of front tier servers
		if (frontTierServers.size() <= MIN_FRONT_TIER_SERVERS) {
			return false;
		}
		
		// get time passed since last scale in
		long currTime = System.currentTimeMillis();
		long timePassed = currTime - lastFrontScaleInTime;
		
		// if enough time since last scale in, approves to terminate the slave server
		if (timePassed >= SCALE_IN_INTERVAL) {
			lastFrontScaleInTime = currTime;
			frontTierServers.remove(vmID);
			serverRoles.remove(vmID);
			return true;
		}
		
		// if not enough time since last scale in, refuses to terminate the slave server
		else {
			return false;
		}
	}
	
	/**
	 * Determines whether the middle-tier server can terminate.
	 * This method is called by the middle-tier server when it thinks it should
	 * terminate due to slow request arrival rate.
	 * The master server returns true and allows it to terminate if the server
	 * is not a master server, and its termination request is not too close to the
	 * last middle-tier server termination.
	 * The master server returns false and refuses it to terminate if the server
	 * is master server, or its termination request is too close to the last middle-tier
	 * server termination.
	 * 
	 * @param vmID, the virtual memory ID of the server asking for termination
	 * @return true, if the master approves the termination; false, otherwise
	 */
	private boolean stopMidTierSlaveServer(int vmID) {
		
		// cannot have less than minimum number of middle tier servers
		if (midTierServers.size() <= MIN_MID_TIER_SERVERS) {
			return false;
		}
		
		// get time passed since last scale in
		long currTime = System.currentTimeMillis();
		long timePassed = currTime - lastMidScaleInTime;
		
		// if enough time since last scale in, approves to terminate the slave server
		if (timePassed >= SCALE_IN_INTERVAL) {
			lastMidScaleInTime = currTime;
			midTierServers.remove(vmID);
			serverRoles.remove(vmID);
			return true;
		}
		
		// if not enough time since last scale in, refuses to terminate the slave server
		else {
			return false;
		}
	}
	
	/**
	 * Determines whether the cache server can terminate.
	 * This method always returns false since a cache should not be scaled.
	 * Actually, this method is a dummy method and will not be called.
	 * 
	 * @param vmID, the virtual memory ID of the server
	 * @return false
	 */
	private boolean stopCacheTierSlaveServer(int vmID) {
		// nothing to do, always return true
		return false;
	}
	

	/**
	 * Calculate the initial number of front-tier servers and middle-tier servers.
	 * The formula is based on benchmarking result.
	 * 
	 * @param numServer, the number of servers to start with given the hour
	 */
	private static void calculateTierDistr(int numServer) {
		initFrontTierServers = Math.max(numServer / 3, MIN_FRONT_TIER_SERVERS);
		initMiddleTierServers = 2 * Math.max(numServer / 3, MIN_MID_TIER_SERVERS);
	}
	
	/**
	 * Let the process run as a front-tier server.
	 * 
	 * A general front-tier server does the following:
	 * 1. Register front end with the Cloud to start receiving clients
	 * 2. Continuously receives requests from the Cloud and sends the requests to the central
	 * queue on the master server that queues all the requests polled by all front-tier servers.
	 * 3. Scale In : If the front-tier server continuously gets slow request arrivals, the server
	 * requests the master server to terminate itself.
	 * 
	 * A master front-tier server also does the following:
	 * 1. Scale Out : Periodically checks if need to scale out the servers.
	 * 
	 * @throws RemoteException
	 */
	private static void runAsFrontTierServer() throws RemoteException {
		
		// start receiving requests from the Cloud
		SL.register_frontend();
		
		// continuously fetch request from the client and process it
		while (true) {
			
			Cloud.FrontEndOps.Request r = SL.getNextRequest();

			try {
				// add request to central queue on the master
				masterServer.addRequest(new RequestInfo(r, System.currentTimeMillis()));
				
				// calculate time elapsed since last request arrival
				long currTime = System.currentTimeMillis();
				long timePassed = currTime - lastRequestArrivalTime;
				
				// if this arrival is too slow
				if (timePassed > MAX_FRONT_ARRIVAL_INTERVAL) {
					
					numContSlowArrivals += 1;
					
					// ask the master for approval of self termination
					if (masterServer.terminateServer(selfServer.getVMID())) {
						// unregister and terminate
						SL.unregister_frontend();
						SL.endVM(selfServer.getVMID());
						UnicastRemoteObject.unexportObject(selfServer, true);
						return;
					}
				}
				
				// if this arrival is a little slow, increase continuous slow arrivals
				else if (timePassed > SLOW_FRONT_ARRIVAL_INTERVAL) {
					
					numContSlowArrivals += 1;

					// stop the front end server if continuous slow arrivals
					if (numContSlowArrivals > MAX_SLOW_ARRIVALS) {
						
						// if master server approves it to terminate
						if (masterServer.terminateServer(selfServer.getVMID())) {
							// unregister and terminate
							SL.unregister_frontend();
							SL.endVM(selfServer.getVMID());
							UnicastRemoteObject.unexportObject(selfServer, true);
							return;
						}
						
						// if master refuses
						else {
							numContSlowArrivals = 0;
						}
					}
				}
				
				// if the request is not slow
				else {
					numContSlowArrivals = 0;
				}
				
				// update last request arrival time
				lastRequestArrivalTime = currTime;
				
				
			}
			
			catch (RemoteException e) {
				System.err.println("RemoteException when adding request to master server:");
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
			
			// scale out if is master server
			if (selfServer.isMasterServer()) {
				
				// get time elapsed since last scaling
				long currTime = System.currentTimeMillis();
				long timePassed = currTime - lastScaleOutTime;
				
				// scale if enough time since last scale and request queue too long
				if (timePassed > SCALE_OUT_INTERVAL) {
					scaleOut();
					lastScaleOutTime = currTime;
				}
			}
			
		}
	}
	
	/**
	 * Let the process run as a middle-tier server.
	 * A middle-tier server does the following:
	 * 1. Continuously fetches requests from the central queue on the master, and sends the requests
	 * to the Cloud for processing
	 * 2. Scale In : If the middle-tier server continuously gets slow arrivals, it asks the master
	 * server for self termination.
	 */
	private static void runAsMidTierServer() {
		
		// continuously fetches requests from the central queue
		while (true) {
			
			RequestInfo r;
			
			try {
				// fetches from the request queue
				r = masterServer.getRequest();
				
				// calculate time elapsed since last request
				long currTime = System.currentTimeMillis();
				long timePassed = currTime - lastRequestArrivalTime;
				
				// if this arrival is too low
				if (r == null) {

					// ask the master for approval of self termination
					if (masterServer.terminateServer(selfServer.getVMID())) {
						
						// end self server
						SL.endVM(selfServer.getVMID());
						UnicastRemoteObject.unexportObject(selfServer, true);
						return;
					}
					
					// if master refuses
					else {
						// reinitialize count for slow arrival
						numContSlowArrivals = 0;
					}
				}
				
				// if the arrival is a little slow, increase continuous slow arrivals
				else if (timePassed > SLOW_MID_ARRIVAL_INTERVAL) {
					
					numContSlowArrivals += 1;
					
					// stop this server if continuous slow arrivals
					if (numContSlowArrivals >= MAX_SLOW_ARRIVALS) {
						
						// if master server approves it to terminate
						if (masterServer.terminateServer(selfServer.getVMID())) {
							// ensure that all requests are processed
							if (r != null) {
								SL.processRequest(r.getRequest(),
										  (Cloud.DatabaseOps) masterServer);
							}
							// end self server
							SL.endVM(selfServer.getVMID());
							UnicastRemoteObject.unexportObject(selfServer, true);
							return;
						}
						
						// if master refuses
						else {
							// count slow arrivals from the beginning
							numContSlowArrivals = 0;
						}
					}
				}
				
				// if the request arrival is not slow
				else {
					numContSlowArrivals = 0;
				}
				
				// process the request
				if (r != null) {
					
					// update the last request arrival time
					lastRequestArrivalTime = currTime;
					
					// process the request
					SL.processRequest(r.getRequest(), (Cloud.DatabaseOps) masterServer);
				}
				
			}
			
			// error handling
			catch (RemoteException e) {
				System.err.println("RemoteException when getting request from master server:");
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Let the process run as a cache server.
	 * A cache server is used by the middle-tier servers and passed to the Cloud. Itself
	 * basically does nothing.
	 */
	private static void runAsCacheTierServer() {
		// does nothing, return
		return;
	}
	
	
	
	/**
	 * Main sets up the server so that it is ready to work in its own role.
	 * 
	 * Main is called every time a virtual machine is started. When main is called, the processes
	 * connects with the Cloud through ServerLib, and prepares itself for running.
	 * 
	 * A server can work as three different roles - front-tier server, middle-tier server,
	 * and cache-tier server.
	 * A front-tier server will receive requests from the Cloud.
	 * A middle-tier server will fetch requests from the front-tier servers, and sends these
	 * requests to the Cloud for processing.
	 * A cache-tier server passed by the middle-tier servers to the Cloud to improve performance.
	 * 
	 * A server can also be a master server or a slave server.
	 * There is only one master server, which coordinates all other slave servers. A master server
	 * always works as a front-tier server, and assigns roles to its slave servers.
	 * There can be multiple slave servers. A slave server gets its role from the master server,
	 * and perform its tasks after setup.
	 * 
	 * @param args, command line arguments.
	 * 		There are three arguments required : <cloud_ip> <cloud_port> <VM id>
	 * @throws Exception
	 */
	public static void main ( String args[] ) throws Exception {

		/* ---------- master server setup ---------- */
		
		// read the command line arguments
		if (args.length != 3) {
			throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
		}
		
		// construct ServerLib object for cloud VM service access
		cloudIP = args[0];
		cloudPort = Integer.parseInt(args[1]);
		vmID = Integer.parseInt(args[2]);
		
		// create the ServerLib object to communicate with the Cloud
		SL = new ServerLib(cloudIP, cloudPort);

		// create the server object for this server itself
		selfServer = new Server();
		
		// bind this server with the RMI
		registry = LocateRegistry.getRegistry(cloudPort);
		registry.bind("Server" + vmID, selfServer);
		
		/* ---------- master server setup ---------- */
		
		if (selfServer.isMasterServer()) {
						
			/* --------- create master server --------- */

			masterServer = selfServer;
			serverRoles.put(MASTER_SERVER_VMID, ServerRole.FRONT_TIER);
			frontTierServers.put(MASTER_SERVER_VMID, ServerRole.FRONT_TIER);

			/* --------- create the cache --------- */
			
			databaseServer = SL.getDB();

			/* --------- create front and middle tier slave servers --------- */
			
			// calculate number of slaves to start with
			int hour = Math.round(SL.getTime());
			int numServer = ServerNumController.getServerNum(HourArrivalRate.getHourRate(hour));
			calculateTierDistr(numServer);
			
			// start the front tier servers
			for (int i = 0; i < (initFrontTierServers - 1); i ++) {
				int slaveVMID = SL.startVM();
				serverRoles.put(slaveVMID, ServerRole.FRONT_TIER);
				frontTierServers.put(slaveVMID, ServerRole.FRONT_TIER);
			}
			
			// start the middle tier servers
			for (int i = 0; i < (initMiddleTierServers); i ++) {
				int slaveVMID = SL.startVM();
				serverRoles.put(slaveVMID, ServerRole.MID_TIER);
				midTierServers.put(slaveVMID, ServerRole.MID_TIER);
			}

		}
		
		/* ---------- slave server setup ---------- */
		
		else {
			// get master server
			masterServer = (ServerInterf) registry.lookup("Server" + MASTER_SERVER_VMID);

		}
		
		/* ---------- assign server role and start the server ---------- */
		
		// get the role of the server
		role = masterServer.assignServerRole(vmID);
		if (role == null) {
			masterServer.forceTerminateServer(vmID);
			SL.endVM(vmID);
			UnicastRemoteObject.unexportObject(selfServer, true);
		}
		
		// let the server perform its task
		if (role.equals(ServerRole.FRONT_TIER)) {
			runAsFrontTierServer();
		}
		else if (role.equals(ServerRole.MID_TIER)) {
			runAsMidTierServer();
		}
		else {
			runAsCacheTierServer();
		}

	}


}



