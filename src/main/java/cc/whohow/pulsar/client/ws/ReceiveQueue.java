package cc.whohow.pulsar.client.ws;


import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ReceiveQueue<T, E> {
    protected final Lock lock = new ReentrantLock();
    protected final Queue<E> queue = new ConcurrentLinkedQueue<>();
    protected final Queue<CompletableFuture<T>> backlog = new ConcurrentLinkedQueue<>();

    public boolean available() {
        return !queue.isEmpty();
    }

    public CompletableFuture<T> receiveAsync() {
        CompletableFuture<T> future = new CompletableFuture<>();
        backlog.offer(future);
        doReceive();
        return future;
    }

    public void received(T e) {
        queue.offer(wrap(e));
        doReceive();
    }

    public void received(Throwable e) {
        queue.offer(wrap(e));
        doReceive();
    }

    public void clear() {
        lock.lock();
        try {
            Throwable closeError = new IllegalStateException();
            queue.clear();
            while (!backlog.isEmpty()) {
                backlog.poll().completeExceptionally(closeError);
            }
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<Void> all() {
        return CompletableFuture.allOf(backlog.toArray(CompletableFuture[]::new));
    }

    protected void doReceive() {
        if (lock.tryLock()) {
            try {
                while (!queue.isEmpty() && !backlog.isEmpty()) {
                    complete(queue.poll(), backlog.poll());
                }
            } finally {
                lock.unlock();
            }
        }
    }

    protected abstract void complete(E e, CompletableFuture<T> f);

    protected abstract E wrap(T e);

    protected abstract E wrap(Throwable e);

    public static class S<T> extends ReceiveQueue<T, T> {
        @Override
        protected T wrap(T e) {
            return e;
        }

        @Override
        protected T wrap(Throwable e) {
            throw new IllegalStateException();
        }

        @Override
        protected void complete(T e, CompletableFuture<T> f) {
            f.complete(e);
        }
    }

    public static class Ex<T> extends ReceiveQueue<T, Result<T>> {
        @Override
        protected Result<T> wrap(T e) {
            return new Result<>(e);
        }

        @Override
        protected Result<T> wrap(Throwable e) {
            return new Result<>(e);
        }

        @Override
        protected void complete(Result<T> result, CompletableFuture<T> future) {
            if (result.isError()) {
                future.completeExceptionally(result.getError());
            } else {
                future.complete(result.getValue());
            }
        }
    }

    static final class Result<T> {
        private final T value;
        private final Throwable error;

        public Result(T value) {
            this.value = value;
            this.error = null;
        }

        public Result(Throwable error) {
            this.value = null;
            this.error = error;
        }

        public T getValue() {
            return value;
        }

        public Throwable getError() {
            return error;
        }

        public boolean isError() {
            return error != null;
        }
    }
}
