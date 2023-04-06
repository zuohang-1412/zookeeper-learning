package org.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class testZK {
    public static void main(String[] args) throws Exception {

        // 异步回调可以使用：
        final CountDownLatch downLatch = new CountDownLatch(1);

        /*
        watch 分为2种：
        1. new zk 时,传入的watch 为session级别， 跟path 和 node 没有关系
        2.
         */
        // 30000 毫秒 session 消亡时间
        final ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 30000, new Watcher() {
            // 回调方法
            @Override
            public void process(WatchedEvent watchedEvent) {
                Event.KeeperState state = watchedEvent.getState();
                Event.EventType type = watchedEvent.getType();
                String path = watchedEvent.getPath();

                System.out.println(watchedEvent.toString());

                switch (state) {
                    case Unknown:
                        break;
                    case Disconnected:
                        break;
                    case NoSyncConnected:
                        break;
                    case SyncConnected:
                        System.out.println("connected...");
                        downLatch.countDown();
                        break;
                    case AuthFailed:
                        break;
                    case ConnectedReadOnly:
                        break;
                    case SaslAuthenticated:
                        break;
                    case Expired:
                        break;
                    case Closed:
                        break;
                }

                switch (type) {
                    case None:
                        break;
                    case NodeCreated:
                        break;
                    case NodeDeleted:
                        break;
                    case NodeDataChanged:
                        break;
                    case NodeChildrenChanged:
                        break;
                    case DataWatchRemoved:
                        break;
                    case ChildWatchRemoved:
                        break;
                    case PersistentWatchRemoved:
                        break;
                }

            }
        });

        // 阻塞
        downLatch.await();

        ZooKeeper.States state = zk.getState();
        switch (state) {
            case CONNECTING:
                System.out.println("CONNECTING...");
                break;
            case ASSOCIATING:
                break;
            case CONNECTED:
                System.out.println("CONNECTED...");
                break;
            case CONNECTEDREADONLY:
                break;
            case CLOSED:
                break;
            case AUTH_FAILED:
                break;
            case NOT_CONNECTED:
                break;
        }

        // 创建目录结构
        String pathName = zk.create("/data", "HelloWorld".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // 取数据
        final Stat stat = new Stat();
        byte[] data = zk.getData("/data", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("getData: "+ watchedEvent);
                // true 是 default watch， 重新注册watch
                // this 是 watch 本身
                try {
                    byte[] zkData = zk.getData("/data", this, stat);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }, stat);
        System.out.println(new String(data));

        // 更新数据
        Stat data1 = zk.setData("/data", "HelloZK".getBytes(), 0);

        // 再次更新数据
        Stat data2 = zk.setData("/data", "ZK world".getBytes(), data1.getVersion());
    }
}
