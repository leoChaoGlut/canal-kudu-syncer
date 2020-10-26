package personal.leo.cks.server.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class CuratorService {
    @Autowired
    CuratorFramework curator;

    private ConcurrentHashMap<String, TreeCache> zkPathMapTreeCache = new ConcurrentHashMap<>();

    @PostConstruct
    private void postConstruct() {

    }

    public void addTreeCacheListener(String zkPath, TreeCacheListener treeCacheListener) throws Exception {
        TreeCache treeCache = zkPathMapTreeCache.get(zkPath);
        if (treeCache == null) {
            treeCache = TreeCache.newBuilder(curator, zkPath).build();
            zkPathMapTreeCache.put(zkPath, treeCache);
            treeCache.start();
        }
        treeCache.getListenable().addListener(treeCacheListener);
    }
}
