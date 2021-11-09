package client;

import utils.KVInterface;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Timestamp;
import java.util.Scanner;
import java.util.UUID;

public class KVClient {

    private static ClientLogger clientLogger = new ClientLogger("KVClient");
    private static Registry[] registryList = new Registry[5];
    private static KVInterface[] kvstubList = new KVInterface[5];
    private static int[] portList = new int[5];

    public static void main(String[] args) throws Exception {

        if (args.length != 6) {
            clientLogger.error("Incorrect argument, please provide 1 host address along " +
                    "with 5 port numbers");
            return;
        }

        String host = args[0];

        for (int i = 1; i < args.length; i += 1) {
            try {
                portList[i - 1] = Integer.parseInt(args[i]);
            } catch (NumberFormatException e) {
                clientLogger.error("Invalid port number");
                return;
            }
            registryList[i - 1] =  LocateRegistry.getRegistry(host, portList[i - 1]);
            kvstubList[i - 1] = (KVInterface) registryList[i - 1].lookup("utils.KVInterface");
        }

        try {

            runHardCodedCommand();

            runCustomCommands();

        } catch(Exception e){
            clientLogger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void execute(KVInterface kvstub, String[] request) {
        if (request[1].equalsIgnoreCase("put")) {
            clientLogger.debug(sendPutRequest(kvstub, request[2], request[3]));
        } else if (request[1].equalsIgnoreCase("get")) {
            clientLogger.debug(sendGetRequest(kvstub, request[2]));
        } else if (request[1].equalsIgnoreCase("delete")) {
            clientLogger.debug(sendDeleteRequest(kvstub, request[2]));
        } else {
            clientLogger.error("Incorrect request command and should not reach here!");
        }
    }

    private static String sendPutRequest(KVInterface kvstub, String key, String value) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        clientLogger.debug("Sending request- PUT " + key + " with Value: " + value + "   " + timestamp);
        try {
            timestamp = new Timestamp(System.currentTimeMillis());
            return kvstub.PUT(UUID.randomUUID(), key, value) + "   " + timestamp;
        } catch (RemoteException e) {
            timestamp = new Timestamp(System.currentTimeMillis());
            return e.getMessage() + "   " + timestamp;
        }
    }

    private static String sendGetRequest(KVInterface kvstub, String key) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        clientLogger.debug("Sending request- GET key: " + key + "   " + timestamp);
        try {
            timestamp = new Timestamp(System.currentTimeMillis());
            return kvstub.GET(UUID.randomUUID(), key) + "   " + timestamp;
        } catch (RemoteException e) {
            timestamp = new Timestamp(System.currentTimeMillis());
            return e.getMessage() + "   " + timestamp;
        }
    }

    private static String sendDeleteRequest(KVInterface kvstub, String key) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        clientLogger.debug("Sending request- DELETE key: " + key + "   " + timestamp);
        try {
            timestamp = new Timestamp(System.currentTimeMillis());
            return kvstub.DELETE(UUID.randomUUID(), key) + "   " + timestamp;
        } catch (RemoteException e) {
            timestamp = new Timestamp(System.currentTimeMillis());
            return e.getMessage() + "   " + timestamp;
        }
    }


    /**
     * Hard-coded operations.
     */
    private static void runHardCodedCommand() {
        String[] put1 = new String[]{"0", "put", "A", "1"};
        String[] put2 = new String[]{"1", "put", "B", "2"};
        String[] put3 = new String[]{"2", "put", "C", "3"};
        String[] put4 = new String[]{"3", "put", "D", "4"};
        String[] put5 = new String[]{"4", "put", "A", "5"};

        String[] get1 = new String[]{"4", "get", "A"};
        String[] get2 = new String[]{"3", "get", "B"};
        String[] get3 = new String[]{"1", "get", "C"};
        String[] get4 = new String[]{"2", "get", "D"};
        String[] get5 = new String[]{"0", "get", "A"};

        String[] del1 = new String[]{"0", "delete", "A"};
        String[] del2 = new String[]{"2", "delete", "B"};
        String[] del3 = new String[]{"4", "delete", "C"};
        String[] del4 = new String[]{"1", "delete", "D"};

        String[] put6 = new String[]{"3", "put", "A", "1"};

        String[] del5 = new String[]{"0", "delete", "A"};

        String[] put7 = new String[]{"2", "put", "B", "3"};

        execute(kvstubList[0], put1);
        execute(kvstubList[1], put2);
        execute(kvstubList[2], put3);
        execute(kvstubList[3], put4);
        execute(kvstubList[4], put5);

        execute(kvstubList[4], get1);
        execute(kvstubList[3], get2);
        execute(kvstubList[1], get3);
        execute(kvstubList[2], get4);
        execute(kvstubList[0], get5);

        execute(kvstubList[0], del1);
        execute(kvstubList[2], del2);
        execute(kvstubList[4], del3);
        execute(kvstubList[1], del4);

        execute(kvstubList[3], put6);
        execute(kvstubList[0], del5);
        execute(kvstubList[2], put7);
    }

    private static void runCustomCommands() {
        while (true) {

            Scanner sc= new Scanner(System.in);
            clientLogger.debug("Enter an operation (PUT/GET/DELETE): ");
            String op = sc.nextLine();

            String[] operation = op.split("\\s+");

            if (operation.length == 1 && operation[0].equalsIgnoreCase("close")) {
                System.exit(0);
            }

            // check if first argument is a number between 0 and 4 to indicate which port,
            // if not, prompt user to input again
            int firstNumber = -1;
            try {
                firstNumber = Integer.parseInt(operation[0]);
            } catch (NumberFormatException e) {
                errorOp();
                continue;
            }

            if (firstNumber < 0 || firstNumber > 4) {
                errorOp();
                continue;
            }

            // check if operation from user is correct
            // if so, send request, if not, prompt user to input operation again
            // and output instructions
            if (operation.length >= 3) {
                if (operation[1].equalsIgnoreCase("PUT") && operation.length == 4) {
                    execute(kvstubList[firstNumber],operation);
                } else if (operation[1].equalsIgnoreCase("GET") && operation.length == 3) {
                    execute(kvstubList[firstNumber],operation);
                } else if (operation[1].equalsIgnoreCase("DELETE") && operation.length == 3) {
                    execute(kvstubList[firstNumber],operation);
                } else {
                    errorOp();
                }
            } else {
                errorOp();
            }
        }
    }

    /**
     * Response to user input error.
     */
    private static void errorOp() {
        String msg = "Operation format incorrect, please follow this format:\n"
                + "0-4 indicating which port + any three of the following:\n"
                + "(1) PUT KEY VAULE\n"
                + "(2) GET KEY\n"
                + "(3) DELETE KEY\n\n"
                + "If you would like to exit, please enter: close";
        clientLogger.debug(msg);
    }
}

