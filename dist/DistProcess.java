import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;

// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher {
    ZooKeeper zk;
    String zkServer, pinfo;

    Worker worker;
    Queue<String> toProcessTaskPathQueue;
    Set<String> seenTaskPathSet;
    Set<String> readyWorkerPathSet;
    Set<String> busyWorkerPathSet;


    DistProcess(String zkhost) {
        worker = null;
        zkServer = zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }


    private Object getField(String path, Object defaultObj) throws IOException {
        try {
            return SerializeLib.deserialize(zk.getData(path, false, null));
        } catch (ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            try {
                zk.create(path, SerializeLib.serialize(defaultObj), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                return defaultObj;
            } catch (KeeperException | InterruptedException e1) {
                e1.printStackTrace();
            }
        }
        return null;
    }

    public void loadFields() throws KeeperException, InterruptedException {

        try {
            toProcessTaskPathQueue = (Queue<String>) getField("/dist21/task-queue", new LinkedList<String>());
            seenTaskPathSet = (Set<String>) getField("/dist21/seen-task-set", new HashSet<String>());
            readyWorkerPathSet = (Set<String>) getField("/dist21/ready-worker-set", new HashSet<String>());
            busyWorkerPathSet = (Set<String>) getField("/dist21/busy-worker-set", new HashSet<String>());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, null, "/dist21/tasks"));
        process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, null, "/dist21/workers"));
    }

    private void writeField(String path, Object obj) {
        try {
            zk.setData(path, SerializeLib.serialize(obj), -1, null, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeFields() {
        writeField("/dist21/task-queue", toProcessTaskPathQueue);
        writeField("/dist21/seen-task-set", seenTaskPathSet);
        writeField("/dist21/ready-worker-set", readyWorkerPathSet);
        writeField("/dist21/busy-worker-set", busyWorkerPathSet);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
        zk = new ZooKeeper(zkServer, 1000, this); //connect to ZK.
        boolean isMaster = false;
        try {
            runForMaster();    // See if you can become the master (i.e, no other master exists)
            loadFields();
            isMaster = true;

        } catch (NodeExistsException nee) {
            worker = new Worker(this, zk, zk.create("/dist21/workers/worker-", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));
            zk.getData(worker.workerPath, worker, null, null); // set worker to watch for updates to its data
            zk.exists("/dist21/master", this, null, null);
        }

        System.out.println("DISTAPP : Role : " + " I will be functioning as " + (isMaster ? "master" : "worker"));

    }

    // Try to become the master.
    void runForMaster() throws UnknownHostException, KeeperException, InterruptedException {
        //Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
        // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        zk.create("/dist21/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    private void assignTasks() {
        System.out.println("Master assigning tasks... [" + readyWorkerPathSet.size() +
                "] available worker(s) and [" + toProcessTaskPathQueue.size() + "] tasks to process" );
        List<String> processedWorkerPaths = new ArrayList<>();
        for (String readyWorkerPath : readyWorkerPathSet) {
            if (toProcessTaskPathQueue.isEmpty()) break;
            String taskPath = toProcessTaskPathQueue.poll();
            System.out.println("Master processed task [" + taskPath + "], assigned to [" + readyWorkerPath + "]");
            processedWorkerPaths.add(readyWorkerPath);
            try {
                zk.setData("/dist21/workers/" + readyWorkerPath, SerializeLib.serialize(taskPath), -1, null, null);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
        readyWorkerPathSet.removeAll(processedWorkerPaths);
        busyWorkerPathSet.addAll(processedWorkerPaths);
    }

    public void process(WatchedEvent e) {
        //Get watcher notifications.

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        System.out.println("DISTAPP : Event received : " + e);
        // Master should be notified if any new znodes are added to tasks.
        if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist21/tasks")) {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the children.
//			getTasks();
            zk.getChildren("/dist21/tasks", this, (int rc, String path, Object ctx, List<String> children) -> {

                System.out.println("master in task callback");

                Set<String> updatedSeenTaskPathSet = new HashSet<>(children);

                for (String taskPath : children) {
                    if (!seenTaskPathSet.contains(taskPath)) toProcessTaskPathQueue.add(taskPath);
                    System.out.println("found task [" + taskPath + "]");
                }

                seenTaskPathSet = updatedSeenTaskPathSet;

                assignTasks();
                writeFields();
            }, null);

        } else if (e.getType() == Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist21/workers")) {
            zk.getChildren("/dist21/workers", this, (int rc, String path, Object ctx, List<String> children) -> {

                System.out.println("master in worker callback");

                Set<String> updatedReadyWorkerPathSet = new HashSet<>();
                Set<String> updatedBusyWorkerPathSet = new HashSet<>();

                // update worker sets

                for (String workerPath : children) {
                    System.out.println("found worker [" + workerPath + "]");
                    if (busyWorkerPathSet.contains(workerPath)) {
                        updatedBusyWorkerPathSet.add(workerPath);
                    } else {
                        System.out.println("worker [" + workerPath + "] is ready");
                        updatedReadyWorkerPathSet.add(workerPath);
                    }
                }

                readyWorkerPathSet = updatedReadyWorkerPathSet;
                busyWorkerPathSet = updatedBusyWorkerPathSet;

                assignTasks();
                writeFields();
            }, null);
        } else if (e.getType() == Event.EventType.NodeDeleted && e.getPath().equals("/dist21/master")) {
            System.out.println("Uh oh, master went down");
            try {
                if (!worker.requestMaster()) {
                    System.out.println("Resetting watch on master...");
                    zk.exists("/dist21/master", this, null, null); // reset the watch on master
                    zk.exists("/dist21/master", this, null, null);

                }
            } catch (InterruptedException | UnknownHostException | KeeperException ex) {
                ex.printStackTrace();
            }
        }

    }

    public static void main(String args[]) throws Exception {
        //Create a new process
        //Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        //Replace this with an approach that will make sure that the process is up and running forever.
        while (true) {
            Thread.sleep(10000);
        }
    }
}
