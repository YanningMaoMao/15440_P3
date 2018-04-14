import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The interface of the Server.
 * The methods are used by communication between the slave servers and the master server.
 * 
 * @author YanningMao
 *
 */
public interface ServerInterf extends Remote {
	
	/**
	 * Gets the virtual memory ID of the server.
	 * 
	 * @return ID, the virtual memory ID of the server.
	 * @throws RemoteException
	 */
	public int getVMID() throws RemoteException;
	
	/**
	 * Checks if the server is a master server.
	 * 
	 * @return true, if the server itself is a master server;
	 * 	   false, otherwise.
	 * @throws RemoteException
	 */
	public boolean isMasterServer() throws RemoteException;
	
	/**
	 * Assigns the role of the server.
	 * A server can have three different roles - front-tier server, middle-tier server,
	 * and cache-tier server.
	 * 
	 * @param id, the virtual memory ID of the server
	 * @return role, the role assigned to the server
	 * @throws RemoteException
	 */
	public ServerRole assignServerRole(int id) throws RemoteException;
	
	/**
	 * Adds the request to the central request queue on the master server.
	 * 
	 * @param r, the request
	 * @throws RemoteException
	 */
	public void addRequest(RequestInfo r) throws RemoteException;
	
	/**
	 * Gets the request from the central request queue on the master server.
	 * 
	 * @return r, the request
	 * @throws RemoteException
	 */
	public RequestInfo getRequest() throws RemoteException;
	
	/**
	 * Determines whether the server with vmID can be terminated.
	 * 
	 * @param vmID, the virtual memory ID of the server asking for termination
	 * @return true, if the master determines that the server can be terminated;
	 * 	   false, otherwise
	 * @throws RemoteException
	 */
	public boolean terminateServer(int vmID) throws RemoteException;
	
	/**
	 * Forces the server to terminate.
	 * The master removes the server from its record of working servers.
	 * 
	 * @param vmID, the virtual memory ID of the server asking for force termination
	 * @throws RemoteException
	 */
	public void forceTerminateServer(int vmID) throws RemoteException;
	
}


