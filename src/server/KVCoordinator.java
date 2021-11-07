package server;

import utils.KVInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class KVCoordinator {
    private static int[] portNumberList = new int[5];
    private static KVServant[] servantList = new KVServant[5];
    private static ServerLogger serverLogger = new ServerLogger("KVCoordinator");

    public static void main(String[] args) {



        if (args.length != 5) {
            serverLogger.error("Please enter 5 port numbers");
        }

        // parse port number
        try {
            for (int i = 0; i < portNumberList.length; i += 1) {
                portNumberList[i] = Integer.parseInt(args[i]);
            }
        } catch (NumberFormatException e) {
            serverLogger.error("Invalid port number format");
        }

        // initiate 5 servants with port numbers
        try {
            for (int i = 0; i < portNumberList.length; i += 1) {
                servantList[i] = new KVServant();
                // **************** port number = 0 ????????
                KVInterface kvStub = (KVInterface) UnicastRemoteObject.exportObject(servantList[i], portNumberList[i]);
                Registry registry = LocateRegistry.createRegistry(portNumberList[i]);
                registry.rebind("utils.KVInterface", kvStub);

                // set up all other port numbers for current servant
                setUpOtherPorts(portNumberList[i]);

                serverLogger.debug("Server " + i + " is listening at port " + portNumberList[i] + " ...");
            }
        } catch (RemoteException e) {
            serverLogger.error("Error creating servants. " + e.getMessage());
        }
    }

    private static void setUpOtherPorts(int currPort) {
        try {
            Registry registry =  LocateRegistry.getRegistry(currPort);
            KVInterface kvStub = (KVInterface) registry.lookup("utils.KVInterface");

            int i = 0;
            int[] otherServantPorts = new int[portNumberList.length - 1];
            for (int portNumber : portNumberList) {
                if (currPort != portNumber) {
                    otherServantPorts[i] = portNumber;
                    i += 1;
                }
            }

            kvStub.setUpServant(otherServantPorts, currPort);

        } catch (RemoteException | NotBoundException e) {
            serverLogger.error("Error setting up other ports. " + e.getMessage());
        }
    }

}
