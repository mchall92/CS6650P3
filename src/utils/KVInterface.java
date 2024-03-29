package utils;

import server.ACKState;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

public interface KVInterface extends Remote {
    String PUT(UUID operationId, String key, String value) throws RemoteException;

    String GET(UUID operationId, String key) throws RemoteException;

    String DELETE(UUID operationId, String key) throws RemoteException;

    void setUpCoordinator(int _coordinatorPort) throws RemoteException;

    void setUpServant(int servantPort) throws RemoteException;

    void setUpCurrentServer(int currentPort, int coordinatorPortNumber, String coordinatorIP) throws RemoteException;

    void prepareKeyValue(UUID operationId, String action, String key, String value) throws RemoteException;

    void goKeyValue(UUID operationId) throws RemoteException;

    void acknowledgeCoordinator(UUID operationId, int otherServant, ACKState ackState) throws RemoteException;

    boolean startToPrepare(UUID operationId, String action, String key, String value) throws RemoteException;

    boolean startToGo(UUID operationId) throws RemoteException;
}
