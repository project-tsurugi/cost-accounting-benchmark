package com.example.nedo.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TreeExample {

	public static void main(String[] args) {
		// TreeExample tree = new TreeExample(72_000, 75_000);
		TreeExample tree = new TreeExample(21_000, 75_000);
		tree.makeTree();
		System.out.println(tree.get中間数());
		System.out.println(tree.levelMap);
	}

	private final int 製品数;
	private final int 原料数;
	private final Random random = new Random();
	private int 中間数 = 0;
	private final Map<Integer, AtomicInteger> levelMap = new TreeMap<>();

	public TreeExample(int 製品数, int 原料数) {
		this.製品数 = 製品数;
		this.原料数 = 原料数;
	}

	public void makeTree() {
		for (int i = 0; i < 製品数; i++) {
			makeTree(i);
		}
	}

	private void makeTree(int 製品ID) {
		// 最大深さを決定
		// final int targetMaxLevel = random(1, 12) + random(1, 13); // 1700万
		// final int targetMaxLevel = random(1, 12) + random(1, 6) + random(0, 7); //
		// 1140万
		// final int targetMaxLevel = random(1, 6) + random(0, 6) + random(1, 6) +
		// random(0, 7); // 980万
		// final int targetMaxLevel = random(1, 6) + random(0, 6) + random(1, 6) +
		// random(0, 3) + random(0, 4); // 920万
		// final int targetMaxLevel = random(1, 6) + random(0, 3) + random(0, 3) +
		// random(1, 3) + random(0, 3)
		// + random(0, 3) + random(0, 4); // 850万
		// final int targetMaxLevel = random(1, 10) + random(0, 10); // 470万
		final int targetMaxLevel = random(1, 8) + random(0, 8); // 206万

		// ルート要素生成
		Node root = new Node(null, NodeType.製品, 1);
		root.setNodeId(製品ID);

		// 深さが最大深さになるまで要素を追加していく
		List<Node> leafList = new ArrayList<>();
		leafList.add(root);
		int nowMaxLevel = root.getLevel();
		for (;;) {
			Node node = getAndRemoveNodeRandom(leafList);
			int level = node.createChild(leafList);

			nowMaxLevel = Math.max(nowMaxLevel, level);
			if (nowMaxLevel >= targetMaxLevel) {
				break;
			}
		}

		// 葉要素にIDを割り当てる
		for (Node node : leafList) {
			node.assignLeafId();
		}

		中間数 += root.count中間材();
		levelMap.computeIfAbsent(nowMaxLevel, k -> new AtomicInteger(0)).incrementAndGet();
	}

	private int random(int min, int max) {
		int bound = max - min + 1;
		return random.nextInt(bound) + min;
	}

	private Node getAndRemoveNodeRandom(List<Node> leafList) {
		int i = random.nextInt(leafList.size());
		return leafList.remove(i);
	}

	private enum NodeType {
		製品, 中間, 原料
	}

	private class Node {
		private final Node parent;
		private NodeType nodeType;
		private int nodeId;
		private final int level;
		private final List<Node> childList = new ArrayList<>();

		public Node(Node parent, NodeType type, int level) {
			this.parent = parent;
			this.nodeType = type;
			this.level = level;
		}

		public void setNodeId(int nodeId) {
			this.nodeId = nodeId;
		}

		public int getLevel() {
			return level;
		}

		public int createChild(List<Node> leafList) {
			if (nodeType != NodeType.製品) {
				nodeType = NodeType.中間;
			}

			// int size = random(1, 3);
			// int size = random(1, 2);
			int size = random(1, 2) + random(0, 1);
			for (int i = 0; i < size; i++) {
				Node node = new Node(this, NodeType.原料, level + 1);
				childList.add(node);
				leafList.add(node);
			}

			return level + 1;
		}

		public void assignLeafId() {
			assert nodeType == NodeType.原料;

			int id;
			do {
				id = random.nextInt(原料数) + 製品数;
			} while (parent.existsLeafId(id));

			this.nodeId = id;
		}

		public boolean existsLeafId(int id) {
			for (Node child : childList) {
				if (child.nodeId == id) {
					return true;
				}
			}
			return false;
		}

		public int count中間材() {
			int count = 0;
			if (nodeType == NodeType.中間) {
				count++;
			}

			for (Node child : childList) {
				count += child.count中間材();
			}

			return count;
		}
	}

	public int get中間数() {
		return 中間数;
	}
}
