package server;

import utils.KVInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class KVCoordinator {

    private static ServerLogger serverLogger = new ServerLogger("KVCoordinator");

    public static void main(String[] args) {
        if (args.length != 1) {
            serverLogger.error("Please enter one port number for coordinator.");
        }

        // parse port number
        int coordinatorPortNumber = -1;
        try {
            coordinatorPortNumber = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            serverLogger.error("Invalid port number format.");
        }

        try {
            KVServant kvCoordinator = new KVServant();
            KVInterface kvStub = (KVInterface) UnicastRemoteObject.exportObject(kvCoordinator, coordinatorPortNumber);
            Registry registry = LocateRegistry.createRegistry(coordinatorPortNumber);
            registry.rebind("utils.KVInterface", kvStub);

            serverLogger.debug("KVCoordinator is listening at port " + coordinatorPortNumber + " ...");
        } catch (RemoteException e) {
            serverLogger.error("Error creating coordinator.");
            serverLogger.error(e.getMessage());
            e.printStackTrace();
        }

        // set up coordinator port number
        setUpCoordinator(coordinatorPortNumber);
    }

    public static void setUpCoordinator(int coordinatorPortNumber) {
        try {

            Registry registry =  LocateRegistry.getRegistry(coordinatorPortNumber);
            KVInterface kvStubCoordinator = (KVInterface) registry.lookup("utils.KVInterface");

            kvStubCoordinator.setUpCoordinator(coordinatorPortNumber);

        } catch (NotBoundException |RemoteException e) {
            serverLogger.error("Error setting up port number for coordinator.");
            serverLogger.error(e.getMessage());
        }
    }

}
