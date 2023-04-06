package org.zookeeper.configurationcenter;

import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
create /data/AppConf "Data"
程序开始打印 data
set /data/AppConf "NewData"
程序开始打印 NewData
delete /data/AppConf
程序开始打印 conf diu le ....
*/
public class TestZK {
    ZooKeeper zk;
    ZKConf zkConf;
    DefaultWatch defaultWatch;
    MyConf confMsg = new MyConf();
    WatchCallBack watchCallback = new WatchCallBack();

    @Before
    public void conn() {
        zkConf = new ZKConf();
        zkConf.setAddress("127.0.0.1:2181/data");
        zkConf.setSessionTime(10000);
        defaultWatch = new DefaultWatch();
        ZKUtils.setConf(zkConf);
        ZKUtils.setWatch(defaultWatch);
        zk = ZKUtils.getZK();
    }

    @After
    public void close() {
        ZKUtils.closeZK();
    }

    @Test
    public void getConfigFromZK() {

        //程序的配置来源：本地文件系统，数据库，redis，zk。。一切程序可以连接的地方
        //配置内容的提供、变更、响应：本地，数据库等等，都是需要心跳判断，或者手工调用触发

        //我是程序A 我需要配置：1，zk中别人是不是填充了配置；2，别人填充、更改了配置之后我怎么办

        watchCallback.setConfMsg(confMsg);
        watchCallback.setInit(1);
        watchCallback.setZk(zk);
        watchCallback.setWatchPath("/AppConf");
        try {
            watchCallback.aWait();
            /*
            1. 节点存在
            2. 节点不存在
            *
            */
            while (true) {
                if (confMsg.getConf().equals("") || null == confMsg.getConf()) {
                    System.err.println("conf diu le ....");
                    watchCallback.aWait();
                } else {
                    System.out.println(confMsg.getConf());
                }
                Thread.sleep(200);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}