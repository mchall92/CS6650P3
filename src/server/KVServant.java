package server;

import utils.KVInterface;

import java.rmi.RemoteException;
import java.util.UUID;

public class KVServant implements KVInterface {

    private ServerLogger serverLogger = new ServerLogger("KVServant");
    private int[] otherServantPorts;
    private int currPort;
    private KeyValue keyValue = new KeyValue();

    @Override
    public String PUT(UUID operationID, String key, String value) throws RemoteException {
        return null;
    }

    @Override
    public String GET(UUID operationID, String key) throws RemoteException {


        return null;
    }

    @Override
    public String DELETE(UUID operationID, String key) throws RemoteException {
        return null;
    }

    @Override
    public void setUpServant(int[] otherServantPort, int currPort) throws RemoteException {
        this.otherServantPorts = otherServantPort;
        this.currPort = currPort;
    }
}
