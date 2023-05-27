package com.djl.study;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {

        final int timeoutMs = 5 * 1000;
        final int sessionTimeoutMs = 60 * 1000;
        final String connectString = "localhost:2181";
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .retryPolicy(new RetryNTimes(1, 1000))
                .connectionTimeoutMs(timeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs);

        // 通过builder产生一个zk-client对象
        CuratorFramework client = builder.build();

        // 设置连接监听器
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                System.out.println("connectionState = " + connectionState);
            }
        });

        // 开启客户端,猜测内部应该是去建立连接
        client.start();

        // 阻塞等待连接建立成功
        boolean connected = client.blockUntilConnected(timeoutMs, TimeUnit.MILLISECONDS);
        if (!connected) {
            throw new IllegalStateException("zookeeper not connected");
        }
        System.out.println("connected ok");

        // 创建path以及写入数据
        final String path = "/name";
        if (client.checkExists().forPath(path) == null) {
            client.create().forPath(path, "djl".getBytes(StandardCharsets.UTF_8));
        }

        final byte[] bytes = client.getData().forPath(path);
        final String data = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("init data = " + data);

        // NodeCache对一个节点进行监听，监听事件包括指定路径的增删改操作
        NodeCache nodeCache = new NodeCache(client, path, false);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("path = " + nodeCache.getPath() + " data changed = " + new String(nodeCache.getCurrentData().getData(), StandardCharsets.UTF_8));
            }
        });
        // 如果为true则首次不会缓存节点内容到cache中，默认为false,设置为true首次不会触发监听事件
        nodeCache.start(true);

        // --------------------------------------

        final String path2 = "/name2";

        if (client.checkExists().forPath(path2) == null) {
            client.create().creatingParentsIfNeeded().forPath(path2, "wangjie".getBytes(StandardCharsets.UTF_8));
        }

        final String data2 = new String(client.getData().forPath(path2), StandardCharsets.UTF_8);
        System.out.println("data2 = " + data2);

        //对指定路径节点的[一级子目录]监听，[不对该节点的操作监听]，[对其子目录的增删改操作监听]
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path2, true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("pathChildrenCacheEvent = " + pathChildrenCacheEvent);
            }
        });
        pathChildrenCache.start();
        System.out.println("开启监听 path = " + path2 + " 对其一级子目录的增删改操作监听 ");

        // treeCache综合NodeCache和PathChildrenCahce的特性，是对[整个目录进行监听]，可以设置监听深度。
        final String path3 = "/name3";
        TreeCache treeCache = new TreeCache(client, path3);
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("treeCacheEvent = " + treeCacheEvent);
            }
        });
        treeCache.start();
        System.out.println("开启监听 path = " + path3 + " 整个目录进行监听 ");

        System.in.read();

        client.close();
    }
}
