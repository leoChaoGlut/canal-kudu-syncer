package personal.leo.cks.server;

import org.junit.Test;
import personal.leo.cks.server.exception.FatalException;

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
    public void test3() throws ExecutionException, InterruptedException {
        final FatalException e = new FatalException();
        System.out.println(e.getClass().getSimpleName().equals(FatalException.class.getSimpleName()));
        System.out.println(e instanceof RuntimeException);
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
