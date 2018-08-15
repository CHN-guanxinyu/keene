package niuke;

import java.util.HashMap;
import java.util.Map;

class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;

    TreeNode(int x) {
        val = x;
    }
}

public class ReConstructBinaryTree {
    public TreeNode reConstructBinaryTree(int[] pre, int[] in) {
        if (pre.length == 0) return null;
        if (pre.length == 1) return new TreeNode(pre[0]);

        //初始化
        int len = pre.length;
        TreeNode[] nodeArr = new TreeNode[len];
        Map<Integer,Integer> indices = new HashMap();
        for (int i = 0; i < len; i++) {
            indices.put(in[i], i);
            nodeArr[i] = new TreeNode(in[i]);
        }

        //核心
        int pi, qi;
        for (int p = 0; p < len - 1; p++) {
            pi = indices.get(pre[p]);
            qi = indices.get(pre[p + 1]);
            int t = qi;
            if (pi < qi) {
                TreeNode tn;
                while ( t > pi && (tn = nodeArr[t]).left == tn.right ) t--;
                nodeArr[t].right = nodeArr[qi];
            } else nodeArr[pi].left = nodeArr[qi];
        }
        return nodeArr[indices.get(pre[0])];
    }
}
