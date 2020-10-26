package personal.leo.cks.server;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import personal.leo.cks.server.constants.ZkPath;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class CommonTest {

    @Test
    public void test() {
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(2);
        queue.add(1);
        System.out.println(queue.remainingCapacity());
        queue.add(1);
        System.out.println(queue.remainingCapacity());
        queue.add(1);
        System.out.println(queue.remainingCapacity());
    }

    @Test
    public void test1() {
        List<Integer> arr = new ArrayList<>(2);
        arr.add(1);
        System.out.println(arr.size());
        arr.add(1);
        System.out.println(arr.size());
        arr.add(1);
        System.out.println(arr.size());
        arr.clear();
        System.out.println(arr.size());
    }

    @Test
    public void test2() throws ExecutionException, InterruptedException {
        final TT tt = new TT();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                futures.add(CompletableFuture.runAsync(tt::m1));
            } else {
                futures.add(CompletableFuture.runAsync(tt::m2));
            }
        }

        for (CompletableFuture<Void> future : futures) {
            future.get();
        }
    }

    @Test
    public void test3() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final CuratorFramework curator = CuratorFrameworkFactory.newClient("test01:2181", retryPolicy);
        curator.start();

//        curator.create().forPath(ZkPath.tableMappingInfo);
        String str = "azkaban,execution_jobs,presto::azkaban.execution_jobs\n";
        curator.setData().forPath(ZkPath.tableMappingInfo, str.getBytes(StandardCharsets.UTF_8));
    }

    public static class TT {
        public void m1() {
            System.out.println("m1");
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public synchronized void m2() {
            System.out.println("m2");
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
