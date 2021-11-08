package utils;

import server.ACKState;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.UUID;

public interface KVInterface extends Remote {
    String PUT(UUID operationId, String key, String value) throws RemoteException;
    String GET(UUID operationId, String key) throws RemoteException;
    String DELETE(UUID operationId, String key) throws RemoteException;
    void setUpServant(int[] otherServantPort, int currPort) throws RemoteException;
    void prepareKeyValue(UUID operationId, String action, String key, String value,
                         int originalServant) throws RemoteException;
    void acknowledgeOriginalServant(UUID operationId, int otherServant, ACKState ackState);
}
