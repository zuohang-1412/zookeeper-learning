package org.zookeeper.locks;

import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zookeeper.configurationcenter.DefaultWatch;
import org.zookeeper.configurationcenter.ZKConf;
import org.zookeeper.configurationcenter.ZKUtils;

public class TestLock {


    ZooKeeper zk;
    ZKConf zkConf;
    DefaultWatch defaultWatch;

    @Before
    public void conn(){
        zkConf = new ZKConf();
        zkConf.setAddress("127.0.0.1:2181/data");
        zkConf.setSessionTime(1000);
        defaultWatch = new DefaultWatch();
        ZKUtils.setConf(zkConf);
        ZKUtils.setWatch(defaultWatch);
        zk = ZKUtils.getZK();
    }

    @After
    public void close(){
        ZKUtils.closeZK();
    }

    @Test
    public void testlock(){
        for (int i = 0; i < 10; i++) {
            // 抛出10个线程
            new Thread(){
                @Override
                public void run() {
                    WatchCallBack watchCallBack = new WatchCallBack();
                    watchCallBack.setZk(zk);
                    String name = Thread.currentThread().getName();
                    watchCallBack.setThreadName(name);

                    try {
                        // 抢锁
                        watchCallBack.tryLock();

                        // 工作
                        System.out.println(name + " at work");
                        watchCallBack.getRootData();
                        // 释放锁
                        watchCallBack.unLock();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }.start();
        }
        while(true){

        }
    }
}