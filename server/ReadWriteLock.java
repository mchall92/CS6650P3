package server;

public class ReadWriteLock {

    private int reading = 0;
    private int writing = 0;
    private int writeRequests = 0;

    public synchronized void lockRead() throws InterruptedException {
        while (writing > 0 || writeRequests > 0) {
            wait();
        }
        reading += 1;
    }

    public synchronized void unlockRead() {
        reading -= 1;
        notifyAll();
    }

    public synchronized void lockWrite() throws InterruptedException {
        writeRequests += 1;
        while (reading > 0 || writing > 0) {
            wait();
        }
        writeRequests -= 1;
        writing += 1;
    }

    public synchronized void unlockWrite() {
        writing -= 1;
        notifyAll();
    }

}
