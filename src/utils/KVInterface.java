package utils;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

public interface KVInterface extends Remote {
    String PUT(UUID operationID, String key, String value) throws RemoteException;
    String GET(UUID operationID, String key) throws RemoteException;
    String DELETE(UUID operationID, String key) throws RemoteException;
    void setUpServant(int[] otherServantPort, int currPort) throws RemoteException;
}
