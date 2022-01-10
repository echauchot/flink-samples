package org.example;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class ThreadingTest {

  public static void main(String[] args) {
    final ExecutorService executor =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new OneThreadFactory());

    executor.execute(
        () -> {
          throw new RuntimeException();
        });
  }

  private static class OneThreadFactory implements ThreadFactory {
    @Nullable private Thread t;

    @Override
    public synchronized Thread newThread(Runnable r) {
      if (t != null) {
        throw new Error(
            "Thread already exists. There should never be more than one "
                + "thread driving the actions of a Source Coordinator. Existing Thread: "
                + t);
      }
      t = new Thread(r, "testThread");
      t.setContextClassLoader(ClassLoader.getSystemClassLoader());
      t.setUncaughtExceptionHandler(new ExceptionHandler());
      return t;
    }
  }

  private static class ExceptionHandler implements Thread.UncaughtExceptionHandler {

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      System.out.println("Throwable caught in executor");
    }
  }
}
