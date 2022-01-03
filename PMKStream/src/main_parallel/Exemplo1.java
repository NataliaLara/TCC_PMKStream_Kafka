package main_parallel;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Exemplo1 {

    private static final ExecutorService pool = Executors.newFixedThreadPool(10);

    public static Callable<String> getData(final int index, final int time) {
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(time);
                return "TESTE-" + index;
            }
        };
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        long start = System.nanoTime();
        Callable<String> c1 = getData(0, 5000);
        Future<String> f1 = pool.submit(c1);
        System.out.println(f1.get());
        long end = System.nanoTime();
        System.out.println("Tempo decorrido (segundos) = " + ((end - start)/1.0E9));
        pool.shutdown();
    }
}
