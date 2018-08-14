package niuke;
/**
 * https://www.nowcoder.com/practice/abc3fe2ce8e146608e868a70efebf62e?tpId=13&tqId=11154&tPage=1&rp=1&ru=%2Fta%2Fcoding-interviews&qru=%2Fta%2Fcoding-interviews%2Fquestion-ranking
 * 二维数组的查找
 *
 * 题目描述
 * 在一个二维数组中（每个一维数组的长度相同），
 * 每一行都按照从左到右递增的顺序排序，每一列
 * 都按照从上到下递增的顺序排序。请完成一个函
 * 数，输入这样的一个二维数组和一个整数，判断
 * 数组中是否含有该整数。
 *
 */

public class BinarySearchOfTwoDimensional {
    public boolean Find(int target, int[][] array) {
        //检查长度
        int xLen = array.length;
        if(xLen == 0) return false;
        int yLen = array[0].length;
        if(yLen == 0) return false;

        return find(target, 0, xLen - 1, 0, yLen - 1, array);
    }

    boolean find(int target, int xl, int xh, int yl, int yh, int[][] map) {


        //退化到一维binarySearch
        if(xl == xh) return yAxisBinarySearch(target, xl, yl, yh, map);
        if(yl == yh) return xAxisBinarySearch(target, yl, xl, xh, map);

        int xm = (xl + xh) >> 1,
                ym = (yl + yh) >> 1,
                p = map[xm][ym];

        //边界
        if(xl == xh && yl == yh) return target == p;

        if (target > p)  return
                find(target, xm + 1, xh, yl, yh, map) ||
                        find(target, xl, xm, ym + 1, yh, map);
        if( target < p) return
                find(target, xl, xm, yl, yh, map) ||
                        find(target, xm + 1, xh, yl, ym, map);

        return true;
    }

    boolean xAxisBinarySearch(int target, int y, int xl, int xh, int[][]map){
        if(xl == xh) return map[xl][y] == target;
        int m = (xl + xh) >> 1;
        if( target > map[m][y] ) return xAxisBinarySearch(target, y, m + 1, xh, map);
        if( target < map[m][y] ) return xAxisBinarySearch(target, y, xl , m, map);
        return true;
    }

    boolean yAxisBinarySearch(int target, int x, int yl, int yh, int[][]map){
        if(yl == yh) return map[x][yl] == target;
        int m = (yl + yh) >> 1;
        if( target > map[x][m] ) return yAxisBinarySearch(target, x, m + 1, yh, map);
        if( target < map[x][m] ) return yAxisBinarySearch(target, x, yl , m, map);
        return true;
    }
}