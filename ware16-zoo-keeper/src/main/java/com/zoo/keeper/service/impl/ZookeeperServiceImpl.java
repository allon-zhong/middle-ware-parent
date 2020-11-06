package com.zoo.keeper.service.impl;

import com.zoo.keeper.config.ZookeeperConfig;
import com.zoo.keeper.service.ZookeeperService;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

@Service
public class ZookeeperServiceImpl implements ZookeeperService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperServiceImpl.class);

    @Override
    public boolean isExistNode(String path) {
        CuratorFramework client = ZookeeperConfig.getClient();
        client.sync() ;
        try {
            Stat stat = client.checkExists().forPath(path);
            return client.checkExists().forPath(path) != null;
        } catch (Exception e) {
            LOGGER.error("isExistNode error...", e);
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void createNode(CreateMode mode, String path) {
        CuratorFramework client = ZookeeperConfig.getClient() ;
        try {
            // 递归创建所需父节点
            client.create().creatingParentsIfNeeded().withMode(mode).forPath(path);

        } catch (Exception e) {
            LOGGER.error("createNode error...", e);
            e.printStackTrace();
        }
    }

    @Override
    public void setNodeData(String path, String nodeData) {
        CuratorFramework client = ZookeeperConfig.getClient() ;
        try {
            // 设置节点数据
            client.setData().forPath(path, nodeData.getBytes("UTF-8"));
        } catch (Exception e) {
            LOGGER.error("setNodeData error...", e);
            e.printStackTrace();
        }
    }

    @Override
    public void createNodeAndData(CreateMode mode, String path, String nodeData) {
        CuratorFramework client = ZookeeperConfig.getClient() ;
        try {
            // 创建节点，关联数据
            client.create().creatingParentsIfNeeded().withMode(mode)
                  .forPath(path,nodeData.getBytes("UTF-8"));
        } catch (Exception e) {
            LOGGER.error("createNode error...", e);
            e.printStackTrace();
        }
    }

    @Override
    public String getNodeData(String path) {
        CuratorFramework client = ZookeeperConfig.getClient() ;
        try {
            // 数据读取和转换
            byte[] dataByte = client.getData().forPath(path) ;
            String data = new String(dataByte,"UTF-8") ;
            if (StringUtils.isNotEmpty(data)){
                return data ;
            }
        }catch (Exception e) {
            LOGGER.error("getNodeData error...", e);
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<String> getNodeChild(String path) {
        CuratorFramework client = ZookeeperConfig.getClient() ;
        List<String> nodeChildDataList = new ArrayList<>();
        try {
            // 节点下数据集
            nodeChildDataList = client.getChildren().forPath(path);
        } catch (Exception e) {
            LOGGER.error("getNodeChild error...", e);
            e.printStackTrace();
        }
        return nodeChildDataList;
    }

    @Override
    public void deleteNode(String path, Boolean recursive) {
        CuratorFramework client = ZookeeperConfig.getClient() ;
        try {
            if(recursive) {
                // 递归删除节点
                client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
            } else {
                // 删除单个节点
                client.delete().guaranteed().forPath(path);
            }
        } catch (Exception e) {
            LOGGER.error("deleteNode error...", e);
            e.printStackTrace();
        }
    }

    @Override
    public InterProcessReadWriteLock getReadWriteLock(String path) {
        CuratorFramework client = ZookeeperConfig.getClient() ;
        // 写锁互斥、读写互斥
        InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(client, path);
        return readWriteLock ;
    }

    /**
     * nodecache监听, 能监听增删改事件, 但获取不到事件类型
     * @param path 节点路径
     * @return void
     */
    @Override
    public NodeCache registerNodeCacheListener(String path){
        try {
            //1. 创建一个NodeCache
            CuratorFramework client = ZookeeperConfig.getClient() ;
            NodeCache nodeCache = new NodeCache(client, path);
            nodeCache.getListenable().addListener(() -> {
                System.out.println("触发监听");
                ChildData childData = nodeCache.getCurrentData();
                if(childData != null){
                    System.out.println("Path: " + childData.getPath());
                    System.out.println("Stat:" + childData.getStat());
                    System.out.println("Data: "+ new String(childData.getData()));
                }
            });

            //3. 启动监听器
            nodeCache.start();

            //4. 返回NodeCache
            return nodeCache;
        } catch (Exception e) {
            LOGGER.error(MessageFormat.format("注册节点监听器出现异常,nodePath:{0}",path),e);
        }
        return null;
    }


    /**
     * watch 监听，一次性的, 而且不能监听到新增节点事件
     * @param path
     */
    @Override
    public void registerNode(String path){
        CuratorFramework client = ZookeeperConfig.getClient() ;
        try {
            // 设置节点监听
            client.getData().usingWatcher(new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    Event.EventType type = watchedEvent.getType();
                    System.out.println("事件类型："+type);
                    System.out.println("Path: " + watchedEvent.getPath());
                    System.out.println("Stat:" + watchedEvent.getState());
                    try {
                        System.out.println("Data: "+ client.getChildren().forPath(path));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).forPath(path);
        } catch (Exception e) {
            LOGGER.error("setNodeListener error...", e);
            e.printStackTrace();
        }
    }

    /**
     * 这个测试没有成功监听到,不知道为啥...
     * @param path 节点路径
     * @return void
     */
    @Override
    public void registerCuratorListener(String path){
        try {
            CuratorFramework client = ZookeeperConfig.getClient() ;
            //1. 创建一个CuratorListener
            CuratorListener listener = new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    System.out.println("监听事件触发，event内容为：" + event.toString());
                }
            };
            client.getCuratorListenable().addListener(listener);
            client.getData().inBackground().forPath(path);
            System.out.println("监听节点成功");

        } catch (Exception e) {
            LOGGER.error(MessageFormat.format("注册节点监听器出现异常,nodePath:{0}",path),e);
        }
    }

    /**
     * 监听父节点下面的子节点的 增删改事件
     * @param parentPath 节点路径
     * @return void
     */
    @Override
    public void registerChildrenCacheListener(String parentPath){
        try {
            CuratorFramework client = ZookeeperConfig.getClient() ;
            //1. 创建一个CuratorListener
            PathChildrenCache pathChildrenCache = new PathChildrenCache(client,parentPath,true);
            pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    System.out.println("事件类型："  + event.getType() + "；操作节点：" + event.getData().getPath());
                }
            });
            System.out.println("监听节点成功");

        } catch (Exception e) {
            LOGGER.error(MessageFormat.format("注册节点监听器出现异常,nodePath:{0}",parentPath),e);
        }
    }

}
