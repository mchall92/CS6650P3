package server;

import utils.KVInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.util.*;

public class KVServant extends Thread implements KVInterface {

    private ServerLogger serverLogger = new ServerLogger("KVServant");
    private List<Integer> servantPorts;
    private int currPort;
    private int coordinatorPort;
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
        serverLogger.debug("Servant port " + this.currPort +
                " : Received request - PUT (key/Value) - " + "(" + key +
                "/" + value + ")" + " from client " + getClient());

        try {
            Registry registry =  LocateRegistry.getRegistry(coordinatorPort);
            KVInterface kvStubCoordinator = (KVInterface) registry.lookup("utils.KVInterface");

            if (!kvStubCoordinator.startToPrepare(operationId, "PUT", key, value)) {
                return "Servant port " + this.currPort + " | operation ID " + operationId +
                        " : Failed to PUT (key/value) - " + key + " / " + value;
            }

            if (!kvStubCoordinator.startToGo(operationId)) {
                return "Servant port " + this.currPort + " | operation ID " + operationId +
                        " : Failed to PUT (key/value) - " + key + " / " + value;
            }

        } catch (NotBoundException e) {
            serverLogger.error("Servant port " + this.currPort + " : Error calling" +
                    "coordinator method during PUT.");
            e.printStackTrace();
        }

        serverLogger.debug("Servant port " + this.currPort + " | operation ID " + operationId +
                " : PUT request SUCCESS. PUT (Key / Value) : (" + key + " / " + value + ")");

        return "PUT request SUCCESS. PUT (Key / Value) : (" + key + " / " + value + ")";
    }

    @Override
    public String GET(UUID operationId, String key) throws RemoteException {
        String message = "";

        serverLogger.debug("Servant port " + this.currPort +
                " : Received request- GET key: " + key + " from client " + getClient());
        try {
            readWriteLock.lockRead();
            String value = "";
            if (keyValue.containsKey(key)) {
                value = keyValue.get(key);
                serverLogger.debug("GET request SUCCESS. GET (Key / Value) -> (" +
                        key + " : " + value + ") for Client: " + getClient());
                message =  "GET request SUCCESS. GET (Key / Value) -> (" +
                        key + " : " + value + ")";
            } else {
                serverLogger.debug("GET request cannot find Key " +
                        key + " for Client: " + getClient());
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
        serverLogger.debug("Servant port " + this.currPort +
                " : Received request - DELETE key - " + key + " from client " + getClient());

        try {
            Registry registry =  LocateRegistry.getRegistry(coordinatorPort);
            KVInterface kvStubCoordinator = (KVInterface) registry.lookup("utils.KVInterface");

            if (!kvStubCoordinator.startToPrepare(operationId, "DELETE", key, "")) {
                return "Servant port " + this.currPort + " | operation ID " + operationId +
                        " : Failed to DELETE (key) - " + key;
            }

            if (!kvStubCoordinator.startToGo(operationId)) {
                return "Servant port " + this.currPort + " | operation ID " + operationId +
                        " : Failed to DELETE (key) - " + key;
            }

        } catch (NotBoundException e) {
            serverLogger.error("Servant port " + this.currPort + " : Error calling" +
                    "coordinator method during DELETE.");
            e.printStackTrace();
        }

        serverLogger.debug("Servant port " + this.currPort + " | operation ID " +
                        operationId + " : DELETE request SUCCESS. DELETE Key: " + key);

        return "DELETE request SUCCESS. DELETE key: " + key;
    }

    @Override
    public void prepareKeyValue(UUID operationId, String action, String key,
                                String value, int originalServant) throws RemoteException {
        serverLogger.debug("Servant port " + this.currPort + " : prepare Key-Value.");
        if (this.tempOperationMap.containsKey(operationId)) {
            sendACKState(operationId, originalServant, ACKState.Prepare);
        }
        addToTempOperationMap(operationId, action, key, value);
        sendACKState(operationId, originalServant, ACKState.Prepare);
    }

    @Override
    public void goKeyValue(UUID operationId, int originalServant) throws RemoteException {
        if (!this.tempOperationMap.containsKey(operationId)) {
            serverLogger.error("Servant port " + this.currPort + " : Should not commit Go " +
                    "without Prepare. Something was wrong.");
        }

        Operation op = this.tempOperationMap.get(operationId);
        try {
            if (op.action.equalsIgnoreCase("PUT")) {
                readWriteLock.lockWrite();
                this.keyValue.put(op.key, op.value);
                readWriteLock.unlockWrite();
            } else if (op.action.equalsIgnoreCase("DELETE")) {
                readWriteLock.lockWrite();
                this.keyValue.delete(op.key);
                readWriteLock.unlockWrite();
            } else {
                serverLogger.error("Servant port " + this.currPort + " : Operation action is " +
                        "invalid. Something was wrong.");
            }
        } catch (InterruptedException e) {
            if (op.action.equalsIgnoreCase("PUT")) {
                serverLogger.error("Servant port " + this.currPort + " : Lock write error for PUT");
            } else if (op.action.equalsIgnoreCase("DELETE")) {
                serverLogger.error("Servant port " + this.currPort + " : Lock write error for DELETE");
            }
            e.printStackTrace();
        }

        this.tempOperationMap.remove(operationId);
        this.sendACKState(operationId, originalServant, ACKState.Go);
    }

    public boolean startToPrepare(UUID operationId, String action, String key, String value){
        serverLogger.debug("Coordinator port " + this.currPort + " : start for Prepare.");
        // add operation to temp operation map
        addToTempOperationMap(operationId, action, key, value);

        // prepare other servants
        this.pendingPrepare.put(operationId, Collections.synchronizedMap(new HashMap<>()));
        for (int otherServant : servantPorts) {
            prepareServants(operationId, action, key, value, otherServant);
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

    private void prepareServants(UUID operationId, String action,
                                 String key, String value, int otherServant) {
        serverLogger.debug("Servant port " + this.currPort +
                " : Sending query to prepare at servant port - " + otherServant +
                " | Operation Id - " + operationId);
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
        serverLogger.debug("Coordinator port " + this.currPort + " : waiting for Prepare.");
        int acknowledgedPrepareCount = 0;
        int tryPrepareCount = 5;

        while (tryPrepareCount > 0) {
            try {
                Thread.sleep(120);
            } catch (InterruptedException e) {
                serverLogger.error("Servant port " + this.currPort + " : Thread sleep for Prepare error.");
            }
            acknowledgedPrepareCount = 0;

            Map<Integer, Acknowledge> acknowledgeMap = this.pendingPrepare.get(operationId);
            for (int otherServant : this.servantPorts) {
                if (acknowledgeMap.get(otherServant).isAcknowledged) {
                    acknowledgedPrepareCount += 1;
                } else {
                    prepareServants(operationId, action, key, value, otherServant);
                }
            }

            if (acknowledgedPrepareCount == servantPorts.size()) {
                return true;
            }

            tryPrepareCount -= 1;
        }
        return false;
    }

    private boolean waitForGo(UUID operationId) {
        serverLogger.debug("Coordinator port " + this.currPort + " : waiting for Go.");
        int acknowledgedGoCount = 0;
        int tryGoCount = 5;

        while (tryGoCount > 0) {
            try {
                Thread.sleep(120);
            } catch (InterruptedException e) {
                serverLogger.error("Servant port " + this.currPort + " : Thread sleep for Go error.");
            }
            acknowledgedGoCount = 0;

            Map<Integer, Acknowledge> acknowledgeMap = this.pendingGo.get(operationId);
            for (int otherServant : this.servantPorts) {
                if (acknowledgeMap.get(otherServant).isAcknowledged) {
                    acknowledgedGoCount += 1;
                } else {
                    goOtherServants(operationId, otherServant);
                }
            }

            if (acknowledgedGoCount == servantPorts.size()) {

                if (!this.tempOperationMap.containsKey(operationId)) {
                    serverLogger.error("Servant port " + this.currPort + " : temp operation" +
                            "map does not contain operation ID. Something was wrong.");
                }

                this.tempOperationMap.remove(operationId);

                return true;
            }

            tryGoCount -= 1;
        }
        return false;
    }

    public boolean startToGo(UUID operationId) {
        this.pendingGo.put(operationId, Collections.synchronizedMap(new HashMap<>()));

        for (int otherServant : this.servantPorts) {
            goOtherServants(operationId, otherServant);
        }

        return waitForGo(operationId);
    }

    private void goOtherServants(UUID operationId, int otherServant) {
        serverLogger.debug("Servant port " + this.currPort +
                " : Sending query to commit Go at other servant port - " + otherServant +
                " | Operation Id - " + operationId);
        try {
            Acknowledge acknowledge = new Acknowledge();
            acknowledge.isAcknowledged = false;
            this.pendingGo.get(operationId).put(otherServant, acknowledge);
            Registry registry = LocateRegistry.getRegistry(otherServant);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");
            kvStub.goKeyValue(operationId, this.currPort);
        } catch (NotBoundException | RemoteException e) {
            serverLogger.error("Error sending query to commit Go at other servants.");
            serverLogger.error(e.getMessage());
        }
    }

    private void sendACKState(UUID operationId, int originalServant, ACKState ackState) {
        serverLogger.debug("Servant port " + this.currPort + " : Sending ACKState- " +
                ackState + " to original servant port " + originalServant);
        try {
            Registry registry = LocateRegistry.getRegistry(originalServant);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");
            kvStub.acknowledgeCoordinator(operationId, this.currPort, ackState);
        } catch (NotBoundException | RemoteException e) {
            serverLogger.debug("Servant port " + this.currPort +
                    " : Error sending acknowledgement back to original port.");
            this.tempOperationMap.remove(operationId);
        }
    }

    @Override
    public void acknowledgeCoordinator(UUID operationId, int otherServant, ACKState ackState) {
        if (ackState == ACKState.Go) {
            this.pendingGo.get(operationId).get(otherServant).isAcknowledged = true;
        } else if (ackState == ACKState.Prepare) {
            this.pendingPrepare.get(operationId).get(otherServant).isAcknowledged = true;
        }
        serverLogger.debug("Servant port " + this.currPort + " : Received ACKState- " +
                ackState + " from other servant port " + otherServant);
    }

    @Override
    public void setUpCoordinator(int _coordinatorPort) {
        this.servantPorts = new ArrayList<>();
        this.coordinatorPort = _coordinatorPort;
        this.currPort = _coordinatorPort;

        serverLogger.debug( "Port number " + _coordinatorPort +
                " : Set up coordinator at port : " + _coordinatorPort);
    }

    @Override
    public void setUpServant(int servantPort) throws RemoteException {
        this.servantPorts.add(servantPort);

        serverLogger.debug( "Port number " + this.coordinatorPort +
                " : Add new server listening at port : " + servantPort);
    }

    @Override
    public void setUpCurrentPort(int currentPort, int coordinatorPortNumber) throws RemoteException {
        this.currPort = currentPort;
        this.coordinatorPort = coordinatorPortNumber;

        serverLogger.debug("Port number " + this.currPort +
                " : Set up current port at " + this.currPort + " and coordinator at " + this.coordinatorPort);
    }

    private String getClient() {
        String client = "";
        try {
            client = RemoteServer.getClientHost();
        } catch (ServerNotActiveException e) {
            serverLogger.error("Error getting client info.");
            serverLogger.error(e.getMessage());
        }
        return client;
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
