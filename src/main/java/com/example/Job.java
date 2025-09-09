package com.example;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;

public class Job<T> {
    private final T data;
    private final Thread worker;
    private final Timer timer = new Timer();
    private Long interval = 1000l;
    private Long start;
    private Long finish;
    private Consumer<Progress> eventListner;
    private Progress progress = new Progress();
    private Exception error;

    public static interface Task<T> {
        void call(T file, Consumer<Progress> progress) throws Exception;
    }

    public static enum Event {
        start,
        progress,
        finish
    }

    public static interface EventListener {
        void emit(Event event, Progress progress);
    }

    public static class Progress {
        public Event event;
        public long max = 1;
        public long current = 0;
        public Progress() {}
        public Progress(long max, long current) {
            this.event = Event.progress;
            this.max = max;
            this.current = current;
        }
        public Progress(Event event, long max, long current) {
            this.event = event;
            this.max = max;
            this.current = current;
        }
    }

    public Job(T data, Task<T> task) {
        this.data = data;
        this.worker = new Thread(() -> {
            try {
                this.start = new Date().getTime();
                this.progress.event = Event.start;
                if (this.eventListner != null) this.eventListner.accept(progress);
                task.call(this.data, progress -> {
                    this.progress = progress;
                });
            } catch (Exception e) {
                this.error = e;
            } finally {
                this.timer.cancel();
                this.finish = new Date().getTime();
                this.progress.event = Event.finish;
                if (this.eventListner != null) this.eventListner.accept(progress);
            }
        });
    }

    public void start() {
        this.worker.start();
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (eventListner != null && progress.event == Event.progress) {
                    eventListner.accept(progress);
                }
            }
        }, this.interval, this.interval);
    }

    public Long getInterval() {
        return interval;
    }

    public void setInterval(Long interval) {
        this.interval = interval;
    }

    public Long getStartTime() {
        return this.start;
    }

    public Long getFinishTime() {
        return this.finish;
    }

    public T getData() {
        return this.data;
    }

    public T getData(boolean join) throws InterruptedException {
        if (join) this.join();
        return this.getData();
    }

    public T getData(long millis) throws InterruptedException {
        this.join(millis);
        return this.getData();
    }

    public Exception getError() {
        return this.error;
    }

    public Progress getProgress() {
        return this.progress;
    }

    public void onUpdate(Consumer<Progress> eventListner) {
        this.eventListner = eventListner;
    }

    public boolean isAlive() {
        return this.worker.isAlive();
    }

    public void join() throws InterruptedException {
        this.join(0L);
    }

    public void join(long millis) throws InterruptedException {
        this.worker.join(millis);
    }
}
