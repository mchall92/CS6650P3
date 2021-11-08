package server;

import utils.KVInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class KVServant extends Thread implements KVInterface {

    private ServerLogger serverLogger = new ServerLogger("KVServant");
    private int[] otherServantPorts;
    private int currPort;
    private KeyValue keyValue = new KeyValue();
    private ReadWriteLock readWriteLock = new ReadWriteLock();

    private Map<UUID, Operation> tempOperationMap =
            Collections.synchronizedMap(new HashMap<>());
    private Map<UUID, Map<Integer, Acknowledge>> pendingPrepare =
            Collections.synchronizedMap(new HashMap<>());
    private  Map<UUID,Map<Integer,Acknowledge>> pendingGo =
            Collections.synchronizedMap(new HashMap<>());


    @Override
    public String PUT(UUID operationId, String key, String value) throws RemoteException {
        if (!startToPrepare(operationId, "PUT", key, value)) {
            return "Servant port " + this.currPort + " | operation ID " + operationId +
                    " : Failed to PUT (key/value) - " + key + " / " + value;
        }


        return null;
    }

    @Override
    public String GET(UUID operationId, String key) throws RemoteException {
        String message = "";
        String client = "";
        try {
            client = RemoteServer.getClientHost();
        } catch (ServerNotActiveException e) {
            serverLogger.error("Error getting client info.");
            serverLogger.error(e.getMessage());
        }

        serverLogger.debug("Received request- GET key: " + key + " from client " + client);
        try {
            readWriteLock.lockRead();
            String value = "";
            if (keyValue.containsKey(key)) {
                value = keyValue.get(key);
                serverLogger.debug("GET request SUCCESS. GET (Key / Value) -> (" +
                        key + " : " + value + ") for Client: " + client);
                message =  "GET request SUCCESS. GET (Key / Value) -> (" +
                        key + " : " + value + ")";
            } else {
                serverLogger.debug("GET request cannot find Key " +
                        key + " for Client: " + client);
                message =  "GET request cannot find Key " + key;
            }
            readWriteLock.unlockRead();
        } catch (InterruptedException e) {
            serverLogger.error("Error processing GET request.");
            serverLogger.error(e.getMessage());
        }
        return message;
    }

    @Override
    public String DELETE(UUID operationId, String key) throws RemoteException {
        if (startToPrepare(operationId, "DELETE", key, "")) {

        } else {

        }

        return null;
    }

    @Override
    public void setUpServant(int[] otherServantPort, int currPort) throws RemoteException {
        this.otherServantPorts = otherServantPort;
        this.currPort = currPort;
    }

    @Override
    public void prepareKeyValue(UUID operationId, String action, String key, String value, int originalServant) throws RemoteException {
        if (this.tempOperationMap.containsKey(operationId)) {
            sendACKState(operationId, originalServant, ACKState.Prepare);
        }
        addToTempOperationMap(operationId, action, key, value);
        sendACKState(operationId, originalServant, ACKState.Prepare);
    }

    private boolean startToPrepare(UUID operationId, String action, String key, String value){
        // add operation to temp operation map
        addToTempOperationMap(operationId, action, key, value);

        // prepare other servants
        this.pendingPrepare.put(operationId, Collections.synchronizedMap(new HashMap<>()));
        for (int otherServant : otherServantPorts) {
            prepareOtherServants(operationId, action, key, value, otherServant);
        }

        return waitForPrepare(operationId, action, key, value);
    }

    private void addToTempOperationMap(UUID _operationId, String _action, String _key, String _value) {
        Operation op = new Operation();
        op.action = _action;
        op.key = _key;
        op.value = _value;
        this.tempOperationMap.put(_operationId, op);
    }

    private void prepareOtherServants(UUID operationId, String action,
                                      String key, String value, int otherServant) {
        serverLogger.debug("Sending query to prepare other servants for UUID: " + operationId);
        try {
            Acknowledge acknowledge = new Acknowledge();
            acknowledge.isAcknowledged = false;
            this.pendingPrepare.get(operationId).put(otherServant, acknowledge);
            Registry registry = LocateRegistry.getRegistry(otherServant);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");
            kvStub.prepareKeyValue(operationId, action, key, value, this.currPort);
        } catch (NotBoundException | RemoteException e) {
            serverLogger.error("Error sending query to prepare other servants.");
            serverLogger.error(e.getMessage());
        }
    }

    private boolean waitForPrepare(UUID operationId, String action, String key, String value) {
        int acknowledgedCount = 0;
        int tryCount = 5;

        while (tryCount > 0) {
            try {
                Thread.sleep(120);
            } catch (InterruptedException e) {
                serverLogger.error("Servant port " + this.currPort + " : Thread sleep for Prepare error.");
            }
            acknowledgedCount = 0;

            Map<Integer, Acknowledge> acknowledgeMap = this.pendingPrepare.get(operationId);
            for (int otherServant : this.otherServantPorts) {
                if (acknowledgeMap.get(otherServant).isAcknowledged) {
                    acknowledgedCount += 1;
                } else {
                    prepareOtherServants(operationId, action, key, value, otherServant);
                }
            }

            if (acknowledgedCount == 4) {
                return true;
            }

            tryCount -= 1;
        }
        return false;
    }

    private void sendACKState(UUID operationId, int originalServant, ACKState ackState) {
        serverLogger.debug("Servant port " + this.currPort + " : Sending ACKState- " +
                ackState + " to original servant port " + originalServant);
        try {
            Registry registry = LocateRegistry.getRegistry(originalServant);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");
            kvStub.acknowledgeOriginalServant(operationId, this.currPort, ackState);
        } catch (NotBoundException | RemoteException e) {
            serverLogger.debug("Servant port " + this.currPort +
                    " : Error sending acknowledgement back to original port.");
            this.tempOperationMap.remove(operationId);
        }
    }

    @Override
    public void acknowledgeOriginalServant(UUID operationId, int otherServant, ACKState ackState) {
        if (ackState == ACKState.Go) {
            this.pendingGo.get(operationId).get(otherServant).isAcknowledged = true;
        } else if (ackState == ACKState.Prepare) {
            this.pendingPrepare.get(operationId).get(otherServant).isAcknowledged = true;
        }
        serverLogger.debug("Servant port " + this.currPort + " : Received ACKState- " +
                ackState + " from other servant port " + otherServant);
    }

    private class Operation {
        String action;
        String key;
        String value;
    }

    private class Acknowledge {
        boolean isAcknowledged;
    }
}
