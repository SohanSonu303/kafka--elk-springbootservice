package com.example.kafka_springboot;

//
//import org.springframework.stereotype.Component;
//
//import java.util.ArrayList;
//import java.util.List;
//
//@Component
//public class MessageBuffer {
//
//    private final List<String> buffer = new ArrayList<>();
//
//    public synchronized void add(String msg) {
//        buffer.add(msg);
//    }
//
//    public synchronized int size() {
//        return buffer.size();
//    }
//
//    public synchronized List<String> drain() {
//        List<String> copy = new ArrayList<>(buffer);
//        buffer.clear();
//        return copy;
//    }
//}

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class MessageBuffer {

    private final Queue<String> queue = new ConcurrentLinkedQueue<>();

    public void add(String msg) {
        queue.add(msg);
    }

    public int size() {
        return queue.size();
    }

    public List<String> drain(int maxItems) {
        List<String> drained = new ArrayList<>();
        for (int i = 0; i < maxItems; i++) {
            String msg = queue.poll();
            if (msg == null) break;
            drained.add(msg);
        }
        return drained;
    }

    public List<String> drainAll() {
        List<String> drained = new ArrayList<>();
        while (true) {
            String msg = queue.poll();
            if (msg == null) break;
            drained.add(msg);
        }
        return drained;
    }
}
