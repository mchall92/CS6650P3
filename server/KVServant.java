package server;

import utils.KVInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.sql.Timestamp;
import java.util.*;

public class KVServant extends Thread implements KVInterface {

    private ServerLogger serverLogger = new ServerLogger("KVServant");
    private List<Integer> servantPorts;
    private int currPort;
    private int coordinatorPort;
    private KeyValue keyValue = new KeyValue();
    private ReadWriteLock readWriteLock = new ReadWriteLock();
    private Timestamp timestamp;

    private Map<UUID, Operation> tempOperationMap =
            Collections.synchronizedMap(new HashMap<>());
    private Map<UUID, Map<Integer, Acknowledge>> pendingPrepare =
            Collections.synchronizedMap(new HashMap<>());
    private  Map<UUID,Map<Integer,Acknowledge>> pendingGo =
            Collections.synchronizedMap(new HashMap<>());


    @Override
    public String PUT(UUID operationId, String key, String value) throws RemoteException {
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Servant port " + this.currPort +
                " : Received request - PUT (key/Value) - " + "(" + key +
                "/" + value + ")" + " from client " + getClient() + "   " + timestamp);

        try {
            Registry registry =  LocateRegistry.getRegistry(coordinatorPort);
            KVInterface kvStubCoordinator = (KVInterface) registry.lookup("utils.KVInterface");

            if (!kvStubCoordinator.startToPrepare(operationId, "PUT", key, value)) {
                timestamp = new Timestamp(System.currentTimeMillis());
                return "Servant port " + this.currPort + " | operation ID " + operationId +
                        " : Failed to PUT (key/value) - " + key + " / " + value + "   " + timestamp;
            }

            if (!kvStubCoordinator.startToGo(operationId)) {
                timestamp = new Timestamp(System.currentTimeMillis());
                return "Servant port " + this.currPort + " | operation ID " + operationId +
                        " : Failed to PUT (key/value) - " + key + " / " + value + "   " + timestamp;
            }

        } catch (NotBoundException e) {
            serverLogger.error("Servant port " + this.currPort + " : Error calling" +
                    "coordinator method during PUT.");
            e.printStackTrace();
        }

        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Servant port " + this.currPort + " | operation ID " + operationId +
                " : PUT request SUCCESS. PUT (Key / Value) : (" + key + " / " + value + ")   " + timestamp);

        return "PUT request SUCCESS. PUT (Key / Value) : (" + key + " / " + value + ")   " + timestamp;
    }

    @Override
    public String GET(UUID operationId, String key) throws RemoteException {
        String message = "";
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Servant port " + this.currPort +
                " : Received request- GET key: " + key + " from client " + getClient() + "   " + timestamp);
        try {
            readWriteLock.lockRead();
            String value = "";
            if (keyValue.containsKey(key)) {
                value = keyValue.get(key);
                timestamp = new Timestamp(System.currentTimeMillis());
                serverLogger.debug("Servant port " + this.currPort +
                        " : GET request SUCCESS. GET (Key / Value) -> (" +
                        key + " : " + value + ") for Client: " + getClient() + "   " + timestamp);
                message =  "GET request SUCCESS. GET (Key / Value) -> (" +
                        key + " : " + value + ")   " + timestamp;
            } else {
                timestamp = new Timestamp(System.currentTimeMillis());
                serverLogger.debug("Servant port " + this.currPort +
                        " : GET request cannot find Key " + key + " for Client: " + getClient() + "   " + timestamp);
                message =  "GET request cannot find Key " + key + "   " + timestamp;
            }
            readWriteLock.unlockRead();
        } catch (InterruptedException e) {
            serverLogger.error("Servant port " + this.currPort + " : Error processing GET request.");
            serverLogger.error(e.getMessage());
        }
        return message;
    }

    @Override
    public String DELETE(UUID operationId, String key) throws RemoteException {
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Servant port " + this.currPort +
                " : Received request - DELETE key - " + key + " from client " + getClient() + "   " + timestamp);

        try {
            Registry registry =  LocateRegistry.getRegistry(coordinatorPort);
            KVInterface kvStubCoordinator = (KVInterface) registry.lookup("utils.KVInterface");

            if (!kvStubCoordinator.startToPrepare(operationId, "DELETE", key, "")) {
                timestamp = new Timestamp(System.currentTimeMillis());
                return "Servant port " + this.currPort + " | operation ID " + operationId +
                        " : Failed to DELETE (key) - " + key + "   " + timestamp;
            }

            if (!kvStubCoordinator.startToGo(operationId)) {
                timestamp = new Timestamp(System.currentTimeMillis());
                return "Servant port " + this.currPort + " | operation ID " + operationId +
                        " : Failed to DELETE (key) - " + key + "   " + timestamp;
            }

        } catch (NotBoundException e) {
            serverLogger.error("Servant port " + this.currPort + " : Error calling" +
                    "coordinator method during DELETE.");
            e.printStackTrace();
        }

        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Servant port " + this.currPort + " | operation ID " +
                        operationId + " : DELETE request SUCCESS. DELETE Key: " + key + "   " + timestamp);

        return "DELETE request SUCCESS. DELETE key: " + key + "   " + timestamp;
    }

    @Override
    public void prepareKeyValue(UUID operationId, String action, String key,
                                String value) throws RemoteException {
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Servant port " + this.currPort + " : prepare Key-Value.   " + timestamp);
        if (this.tempOperationMap.containsKey(operationId)) {
            sendACKState(operationId, ACKState.Prepare);
        }
        addToTempOperationMap(operationId, action, key, value);
        sendACKState(operationId, ACKState.Prepare);
    }

    @Override
    public void goKeyValue(UUID operationId) throws RemoteException {
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
        this.sendACKState(operationId, ACKState.Go);
    }

    public boolean startToPrepare(UUID operationId, String action, String key, String value){
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Coordinator port " + this.currPort + " : start for Prepare.   " + timestamp);
        // add operation to temp operation map
        addToTempOperationMap(operationId, action, key, value);

        // prepare servants
        this.pendingPrepare.put(operationId, Collections.synchronizedMap(new HashMap<>()));
        for (int servantPort : servantPorts) {
            prepareServants(operationId, action, key, value, servantPort);
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
                                 String key, String value, int servantPort) {
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Coordinator port " + this.currPort +
                " : Sending query to prepare at servant port - " + servantPort +
                " | Operation Id - " + operationId + "   " + timestamp);
        try {
            Acknowledge acknowledge = new Acknowledge();
            acknowledge.isAcknowledged = false;
            this.pendingPrepare.get(operationId).put(servantPort, acknowledge);
            Registry registry = LocateRegistry.getRegistry(servantPort);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");
            kvStub.prepareKeyValue(operationId, action, key, value);
        } catch (NotBoundException | RemoteException e) {
            serverLogger.error("Error sending query to prepare servants.");
            serverLogger.error(e.getMessage());
        }
    }

    private boolean waitForPrepare(UUID operationId, String action, String key, String value) {
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Coordinator port " + this.currPort + " : waiting for Prepare.   " + timestamp);
        int acknowledgedPrepareCount = 0;
        int tryPrepareCount = 5;

        while (tryPrepareCount > 0) {
            try {
                Thread.sleep(120);
            } catch (InterruptedException e) {
                serverLogger.error("Coordinator port " + this.currPort + " : Thread sleep for Prepare error.");
            }
            acknowledgedPrepareCount = 0;

            Map<Integer, Acknowledge> acknowledgeMap = this.pendingPrepare.get(operationId);
            for (int servantPort : this.servantPorts) {
                if (acknowledgeMap.get(servantPort).isAcknowledged) {
                    acknowledgedPrepareCount += 1;
                } else {
                    prepareServants(operationId, action, key, value, servantPort);
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
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Coordinator port " + this.currPort + " : waiting for Go.   " + timestamp);
        int acknowledgedGoCount = 0;
        int tryGoCount = 5;

        while (tryGoCount > 0) {
            try {
                Thread.sleep(120);
            } catch (InterruptedException e) {
                serverLogger.error("Coordinator port " + this.currPort + " : Thread sleep for Go error.");
            }
            acknowledgedGoCount = 0;

            Map<Integer, Acknowledge> acknowledgeMap = this.pendingGo.get(operationId);
            for (int servantPort : this.servantPorts) {
                if (acknowledgeMap.get(servantPort).isAcknowledged) {
                    acknowledgedGoCount += 1;
                } else {
                    goServants(operationId, servantPort);
                }
            }

            if (acknowledgedGoCount == servantPorts.size()) {

                if (!this.tempOperationMap.containsKey(operationId)) {
                    serverLogger.error("Coordinator port " + this.currPort + " : temp operation" +
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

        for (int servantPort : this.servantPorts) {
            goServants(operationId, servantPort);
        }

        return waitForGo(operationId);
    }

    private void goServants(UUID operationId, int servantPort) {
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Coordinator port " + this.currPort +
                " : Sending query to commit Go at servant port - " + servantPort +
                " | Operation Id - " + operationId + "   " + timestamp);
        try {
            Acknowledge acknowledge = new Acknowledge();
            acknowledge.isAcknowledged = false;
            this.pendingGo.get(operationId).put(servantPort, acknowledge);
            Registry registry = LocateRegistry.getRegistry(servantPort);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");
            kvStub.goKeyValue(operationId);
        } catch (NotBoundException | RemoteException e) {
            serverLogger.error("Error sending query to commit Go at servants.");
            serverLogger.error(e.getMessage());
        }
    }

    private void sendACKState(UUID operationId, ACKState ackState) {
        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Servant port " + this.currPort + " : Sending ACKState- " +
                ackState + " to coordinator port " + this.coordinatorPort + "   " + timestamp);
        try {
            Registry registry = LocateRegistry.getRegistry(coordinatorPort);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");
            kvStub.acknowledgeCoordinator(operationId, this.currPort, ackState);
        } catch (NotBoundException | RemoteException e) {
            serverLogger.debug("Servant port " + this.currPort +
                    " : Error sending acknowledgement back to coordinator port.");
            this.tempOperationMap.remove(operationId);
        }
    }

    @Override
    public void acknowledgeCoordinator(UUID operationId, int servantPort, ACKState ackState) {
        if (ackState == ACKState.Go) {
            this.pendingGo.get(operationId).get(servantPort).isAcknowledged = true;
        } else if (ackState == ACKState.Prepare) {
            this.pendingPrepare.get(operationId).get(servantPort).isAcknowledged = true;
        }

        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Coordinator port " + this.currPort + " : Received ACKState- " +
                ackState + " from servant port " + servantPort + "   " + timestamp);
    }

    @Override
    public void setUpCoordinator(int _coordinatorPort) {
        this.servantPorts = new ArrayList<>();
        this.coordinatorPort = _coordinatorPort;
        this.currPort = _coordinatorPort;

        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug( "Port number " + _coordinatorPort +
                " : Set up coordinator at port : " + _coordinatorPort + "   " + timestamp);
    }

    @Override
    public void setUpServant(int servantPort) throws RemoteException {
        this.servantPorts.add(servantPort);

        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug( "Port number " + this.coordinatorPort +
                " : Add new server listening at port : " + servantPort + "   " + timestamp);
    }

    @Override
    public void setUpCurrentPort(int currentPort, int coordinatorPortNumber) throws RemoteException {
        this.currPort = currentPort;
        this.coordinatorPort = coordinatorPortNumber;

        timestamp = new Timestamp(System.currentTimeMillis());
        serverLogger.debug("Port number " + this.currPort + " : Set up current port at " +
                this.currPort + " and coordinator at " + this.coordinatorPort + "   " + timestamp);
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
