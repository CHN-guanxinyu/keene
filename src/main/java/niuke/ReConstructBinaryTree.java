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

        /**
         * 先序遍历序列pre中满足:
         * 1.对于任意相邻位置i, j = i + 1对应节点,在中序遍历中的顺序存在两种情况:
         *  a)中序中,j在i前,这时一定满足j是i的左孩子
         *  b)中序中,i在j前,这时分两种情况
         *    1)中序序列i~j位置的中间不存在i和j的公共父节点,j是i的直接右孩子
         *    2)中序序列i~j位置的中间存在i和j的公共父节点,则最靠近j一侧的父节点就是最近的公共父节点,并且j一定是这个公共父节点的右孩子
         *  判断是否是父节点直接看该节点的左,右孩子是否为空即可
         *
         */
        int first, second;
        //遍历pre的所有相邻i,j
        for (int i = 0; i < len - 1; i++) {
            //first,second是两个节点在中序中的位置
            first = indices.get(pre[i]);
            second = indices.get(pre[i + 1]);

            //对应上述b条件
            if (first < second) {
                int t = second;
                TreeNode tn;
                //从second位置反向查找第一个父节点
                //找不到父节点的话t刚好就等于first
                //这里条件做了个优化,
                //查找i,j中间的父节点,父节点左右孩子都为空,正常左右孩子不会相等,如果相等,则一定都为空
                while ( t > first && (tn = nodeArr[t]).left == tn.right ) t--;
                //这里t可能指向的是最近公共父节点,也可能是i(就是first)节点
                nodeArr[t].right = nodeArr[second];
            }
            //对应a条件
            else nodeArr[first].left = nodeArr[second];
        }
        return nodeArr[indices.get(pre[0])];
    }
}
