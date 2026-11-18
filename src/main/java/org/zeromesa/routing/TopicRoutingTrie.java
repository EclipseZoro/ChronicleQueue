package org.zeromesa.routing;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TopicRoutingTrie {
    private static class TrieNode {
        final Map<String, TrieNode> children = new HashMap<>();
        final Set<String> boundGroups = new HashSet<>();
    }

    private final TrieNode root = new TrieNode();
    private final Set<String> fanoutGroups = new CopyOnWriteArraySet<>();
    private final Map<String, Set<String>> directBindings = new ConcurrentHashMap<>();
    private final Map<String, ConsumerGroupQueue> groupQueues = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsumerGroupQueue registerGroup(String groupId, int queueCapacity) {
        return groupQueues.computeIfAbsent(groupId, id -> new ConsumerGroupQueue(id, queueCapacity));
    }

    public void bind(ExchangeType type, String pattern, String groupId) {
        registerGroup(groupId, 10_000);
        switch (type) {
            case FANOUT -> fanoutGroups.add(groupId);
            case DIRECT -> directBindings
                    .computeIfAbsent(pattern, k -> new CopyOnWriteArraySet<>())
                    .add(groupId);
            case TOPIC -> {
                lock.writeLock().lock();
                try {
                    String[] tokens = pattern.split("\\.");
                    TrieNode curr = root;
                    for (String token : tokens) {
                        curr = curr.children.computeIfAbsent(token, k -> new TrieNode());
                    }
                    curr.boundGroups.add(groupId);
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }

    public Set<String> route(long offset, String routingKey, byte[] payload) {
        Set<String> matchedGroups = new HashSet<>(fanoutGroups);

        if (routingKey != null) {
            Set<String> direct = directBindings.get(routingKey);
            if (direct != null) {
                matchedGroups.addAll(direct);
            }

            lock.readLock().lock();
            try {
                String[] tokens = routingKey.split("\\.");
                searchTrie(root, tokens, 0, matchedGroups);
            } finally {
                lock.readLock().unlock();
            }
        }

        ConsumerGroupQueue.RoutedEvent event = new ConsumerGroupQueue.RoutedEvent(offset, routingKey, payload);
        for (String groupId : matchedGroups) {
            ConsumerGroupQueue queue = groupQueues.get(groupId);
            if (queue != null) {
                queue.offer(event);
            }
        }
        return matchedGroups;
    }

    private void searchTrie(TrieNode node, String[] tokens, int index, Set<String> results) {
        if (node == null) return;

        // '#' can match zero remaining tokens at the end
        TrieNode hashNode = node.children.get("#");
        if (hashNode != null) {
            results.addAll(hashNode.boundGroups);
        }

        if (index == tokens.length) {
            results.addAll(node.boundGroups);
            return;
        }

        String token = tokens[index];

        // 1. Exact token match
        TrieNode exactChild = node.children.get(token);
        if (exactChild != null) {
            searchTrie(exactChild, tokens, index + 1, results);
        }

        // 2. Single-word wildcard '*'
        TrieNode starChild = node.children.get("*");
        if (starChild != null) {
            searchTrie(starChild, tokens, index + 1, results);
        }

        // 3. Multi-word wildcard '#' consuming 1 or more tokens
        if (hashNode != null) {
            for (int nextIdx = index + 1; nextIdx <= tokens.length; nextIdx++) {
                searchTrie(hashNode, tokens, nextIdx, results);
            }
        }
    }

    public ConsumerGroupQueue getGroupQueue(String groupId) {
        return groupQueues.get(groupId);
    }
}