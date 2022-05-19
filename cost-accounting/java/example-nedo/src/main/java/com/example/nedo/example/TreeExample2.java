package com.example.nedo.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("unused")
public class TreeExample2 {

    public static void main(String[] args) {
        TreeExample2 tree = new TreeExample2(7_2000, 19_8000, 7_5000);
        tree.makeTrees();
        System.out.println(tree.count中間数);
    }

    private final int 製品数;
    private final int 中間数;
    private final int 原料数;
    private final AtomicInteger 中間材ID;
    private final Random random = new Random();
    private int count中間数 = 0;

    private int random(int min, int max) {
        int bound = max - min + 1;
        return random.nextInt(bound) + min;
    }

    public TreeExample2(int 製品数, int 中間数, int 原料数) {
        this.製品数 = 製品数;
        this.中間数 = 中間数;
        this.原料数 = 原料数;
        this.中間材ID = new AtomicInteger(製品数 + 原料数 + 1);
    }

    public void makeTrees() {
        final int size = 中間数 / 10;
        for (int i = 0; i < size; i++) {
            makeTree();
        }
    }

    private void makeTree() {
        // 木の要素数
        final int targetSize = 10 + random(-4, 4);

        // ルート要素
        Node root = new Node(null, NodeType.中間);

        List<Node> nodeList = new ArrayList<>(targetSize);
        nodeList.add(root);
        while (nodeList.size() < targetSize) {
            Node node = nodeList.get(random.nextInt(nodeList.size()));

            Node child = new Node(node, NodeType.中間);
            node.addChild(child);
            nodeList.add(child);
        }

        count中間数 += nodeList.size();

        // 品目ID割り当て
        root.assign中間材Id();
    }

    private enum NodeType {
        製品, 中間, 原料
    }

    private class Node {
        private final Node parent;
        private NodeType nodeType;
        private int nodeId;
        private final List<Node> childList = new ArrayList<>();

        public Node(Node parent, NodeType nodeType) {
            this.parent = parent;
            this.nodeType = nodeType;
        }

        public void addChild(Node child) {
            assert child.parent == this;
            childList.add(child);
        }

        public void assign中間材Id() {
            this.nodeId = 中間材ID.getAndIncrement();

            for (Node child : childList) {
                child.assign中間材Id();
            }
        }
    }
}
