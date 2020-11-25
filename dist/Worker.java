import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker implements Watcher {

    ZooKeeper zk;
    public String workerPath;
    public final AtomicBoolean processing;


    public Worker(ZooKeeper zk, String workerPath) {
        this.zk = zk;
        this.workerPath = workerPath;
        processing = new AtomicBoolean(false);
    }


    // Implementing the Watcher interface
    @Override
    public void process(WatchedEvent watchedEvent) {

        System.out.println("[" + workerPath + "] received watch: " + watchedEvent);

        if (watchedEvent.getType() != Event.EventType.NodeDataChanged || !watchedEvent.getPath().equals(workerPath)) { // todo more informative error
            return;
        }

        Watcher watcher = this; // for use in the threading library

        System.out.println("[" + workerPath + "] watch for data change triggered");

        zk.getData(workerPath, false, (int rc, String path, Object ctx, byte[] data, Stat stat) -> {
            processing.set(true);

            System.out.println("[" + workerPath + "]: in callback for getData, with data " + Arrays.toString(data));

            if (data == null) {
                return;
            }

            // delete self from ready workers, -1 matches any version
            zk.delete(workerPath, -1, null, null);

            new Thread(() -> {
                try {

//                    String taskPath = new String(data); // todo check this actually does what I want
                    String taskPath = (String) SerializeLib.deserialize(data);

                    System.out.println("[" + workerPath + "]: working on task [" + taskPath + "]");

                    byte[] taskSerial = zk.getData("/dist21/tasks/" + taskPath, false, null);

                    // Re-construct our task object.
                    DistTask dt = (DistTask) SerializeLib.deserialize(taskSerial);

                    //Execute the task.
                    dt.compute();

                    // Serialize our Task object back to a byte array!
                    taskSerial = SerializeLib.serialize(dt);

                    // Store it inside the result node.
                    zk.create("/dist21/tasks/" + taskPath + "/result", taskSerial, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    // add self to ready workers again
                    workerPath = zk.create("/dist21/workers/worker-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                    zk.getData(workerPath, watcher, null, null); // set temporary watch on data

                    System.out.println("[" + workerPath + "]: finished task [" + taskPath + "]");
                    processing.set(false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

        }, null);
    }
}
