package server;

import utils.KVInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;

public class KVServer4 {

    private static ServerLogger serverLogger = new ServerLogger("KVServer4");
    private static Timestamp timestamp;

    public static void main(String[] args) {
        if (args.length != 3) {
            serverLogger.error("Please enter two port numbers: first one indicates " +
                    "this port, second one indicates coordinator port, third one indicate coordinator address.");
        }

        // parse port number
        int servantPortNumber = -1;
        int coordinatorPortNumber = -1;
        String coordinatorAddress = args[2];
        try {
            servantPortNumber = Integer.parseInt(args[0]);
            coordinatorPortNumber = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            serverLogger.error("Invalid port number format");
        }

        try {
            KVServant kvServer4 = new KVServant();
            KVInterface kvStub = (KVInterface) UnicastRemoteObject.exportObject(kvServer4, servantPortNumber);
            Registry registry = LocateRegistry.createRegistry(servantPortNumber);
            registry.rebind("utils.KVInterface", kvStub);

            timestamp = new Timestamp(System.currentTimeMillis());
            serverLogger.debug("KVServer4 is listening at port " + servantPortNumber +
                    " ...   " + timestamp);

            // set up current port for server
            setUpMyPort(servantPortNumber, coordinatorPortNumber, coordinatorAddress);

            // connect to coordinator and register for this server
            registerServer(coordinatorPortNumber, servantPortNumber, coordinatorAddress);


        } catch (RemoteException e) {
            serverLogger.error("Error creating server4.");
            serverLogger.error(e.getMessage());
        }
    }

    private static void registerServer(int coordinatorPortNumber, int servantPortNumber, String coordinatorAddress) {
        try {

            Registry registry =  LocateRegistry.getRegistry(coordinatorAddress, coordinatorPortNumber);
            KVInterface kvStubCoordinator = (KVInterface) registry.lookup("utils.KVInterface");

            kvStubCoordinator.setUpServant(servantPortNumber);

        } catch (NotBoundException |RemoteException e) {
            serverLogger.error("Error registering server to coordinator.");
            serverLogger.error(e.getMessage());
        }
    }

    private static void setUpMyPort(int servantPortNumber, int coordinatorPortNumber, String coordinatorAddress) {
        try {
            Registry registry =  LocateRegistry.getRegistry(servantPortNumber);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");

            kvStub.setUpCurrentServer(servantPortNumber, coordinatorPortNumber, coordinatorAddress);

        } catch (NotBoundException |RemoteException e) {
            serverLogger.error("Error setting up current port.");
            serverLogger.error(e.getMessage());
        }
    }
}
