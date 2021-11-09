package server;

import utils.KVInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class KVServer5 {

    private static ServerLogger serverLogger = new ServerLogger("KVServer5");

    public static void main(String[] args) {
        if (args.length != 2) {
            serverLogger.error("Please enter two port numbers: first one indicates " +
                    "this port, second one indicates coordinator port");
        }

        // parse port number
        int servantPortNumber = -1;
        int coordinatorPortNumber = -1;
        try {
            servantPortNumber = Integer.parseInt(args[0]);
            coordinatorPortNumber = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            serverLogger.error("Invalid port number format");
        }

        try {
            KVServant kvServer5 = new KVServant();
            KVInterface kvStub = (KVInterface) UnicastRemoteObject.exportObject(kvServer5, servantPortNumber);
            Registry registry = LocateRegistry.createRegistry(servantPortNumber);
            registry.rebind("utils.KVInterface", kvStub);

            serverLogger.debug("KVServer5 is listening at port " + servantPortNumber + " ...");

            // set up current port for server
            setUpMyPort(servantPortNumber, coordinatorPortNumber);

            // connect to coordinator and register for this server
            registerServer(coordinatorPortNumber, servantPortNumber);


        } catch (RemoteException e) {
            serverLogger.error("Error creating server5.");
            serverLogger.error(e.getMessage());
        }
    }

    private static void registerServer(int coordinatorPortNumber, int servantPortNumber) {
        try {

            Registry registry =  LocateRegistry.getRegistry(coordinatorPortNumber);
            KVInterface kvStubCoordinator = (KVInterface) registry.lookup("utils.KVInterface");

            kvStubCoordinator.setUpServant(servantPortNumber);

        } catch (NotBoundException |RemoteException e) {
            serverLogger.error("Error registering server to coordinator.");
            serverLogger.error(e.getMessage());
        }
    }

    private static void setUpMyPort(int servantPortNumber, int coordinatorPortNumber) {
        try {
            Registry registry =  LocateRegistry.getRegistry(servantPortNumber);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");

            kvStub.setUpCurrentPort(servantPortNumber, coordinatorPortNumber);

        } catch (NotBoundException |RemoteException e) {
            serverLogger.error("Error setting up current port.");
            serverLogger.error(e.getMessage());
        }
    }
}
