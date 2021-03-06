package com.zoo.keeper.controller;

import com.zoo.keeper.service.ZookeeperService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.zookeeper.CreateMode;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;
import java.util.List;

@Api("Zookeeper接口管理")
@RestController
public class ZookeeperApi {

    @Resource
    private ZookeeperService zookeeperService ;

    @ApiOperation(value="查询节点数据")
    @GetMapping("/getNodeData")
    public String getNodeData (String path) {
        return zookeeperService.getNodeData(path) ;
    }

    @ApiOperation(value="判断节点是否存在")
    @GetMapping("/isExistNode")
    public boolean isExistNode (final String path){
        return zookeeperService.isExistNode(path) ;
    }

    @ApiOperation(value="创建节点")
    @GetMapping("/createNode")
    public String createNode (CreateMode mode, String path ){
        zookeeperService.createNode(mode,path) ;
        return "success" ;
    }

    @ApiOperation(value="设置节点数据")
    @GetMapping("/setNodeData")
    public String setNodeData (String path, String nodeData) {
        zookeeperService.setNodeData(path,nodeData) ;
        return "success" ;
    }

    @ApiOperation(value="创建并设置节点数据")
    @GetMapping("/createNodeAndData")
    public String createNodeAndData (CreateMode mode, String path , String nodeData){
        zookeeperService.createNodeAndData(mode,path,nodeData) ;
        return "success" ;
    }

    @ApiOperation(value="递归获取节点数据")
    @GetMapping("/getNodeChild")
    public List<String> getNodeChild (String path) {
        return zookeeperService.getNodeChild(path) ;
    }

    @ApiOperation(value="是否递归删除节点")
    @GetMapping("/deleteNode")
    public String deleteNode (String path,Boolean recursive) {
        zookeeperService.deleteNode(path,recursive) ;
        return "success" ;
    }

    @ApiOperation(value="注册监听")
    @GetMapping("/registerNodeCacheListener")
    public String registerNodeCacheListener (String path) {
        zookeeperService.registerNodeCacheListener(path);
        return "success" ;
    }

    @ApiOperation(value="注册监听")
    @GetMapping("/addWatchListener")
    public String addWatchListener (String path) {
        zookeeperService.registerNode(path);
        return "success" ;
    }

    @ApiOperation(value="注册监听")
    @GetMapping("/registerCuratorListener")
    public String registerCuratorListener (String path) {
        zookeeperService.registerCuratorListener(path);
        return "success" ;
    }

    @ApiOperation(value="注册监听")
    @GetMapping("/registerChildrenCacheListener")
    public String registerChildrenCacheListener (String path) {
        zookeeperService.registerChildrenCacheListener(path);
        return "success" ;
    }
}
