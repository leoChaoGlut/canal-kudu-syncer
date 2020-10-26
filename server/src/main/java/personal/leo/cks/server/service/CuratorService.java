package personal.leo.cks.server.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class CuratorService {
    @Autowired
    CuratorFramework curator;

    private ConcurrentHashMap<String, TreeCache> zkPathMapTreeCache = new ConcurrentHashMap<>();

    public void addTreeCacheListener(String zkPath, TreeCacheListener treeCacheListener) throws Exception {
        TreeCache treeCache = zkPathMapTreeCache.get(zkPath);
        if (treeCache == null) {
            treeCache = TreeCache.newBuilder(curator, zkPath).build();
            zkPathMapTreeCache.put(zkPath, treeCache);
            treeCache.start();
        }
        treeCache.getListenable().addListener(treeCacheListener);
    }

    public String readData(String zkPath) throws Exception {
        final byte[] bytes = curator.getData().forPath(zkPath);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String readData(String zkPath, Stat stat) throws Exception {
        final byte[] bytes = curator.getData().storingStatIn(stat).forPath(zkPath);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public Stat writeData(String zkPath, String data, int expectedVersion) throws Exception {
        final byte[] bytes = data == null ? null : data.getBytes(StandardCharsets.UTF_8);
        return curator.setData()
                .withVersion(expectedVersion)
                .forPath(zkPath, bytes);
    }
}
