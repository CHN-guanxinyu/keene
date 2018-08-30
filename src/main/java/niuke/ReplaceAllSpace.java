package niuke;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

/**
 * https://www.nowcoder.com/practice/4060ac7e3e404ad1a894ef3e17650423?tpId=13&tqId=11155&tPage=1&rp=1&ru=%2Fta%2Fcoding-interviews&qru=%2Fta%2Fcoding-interviews%2Fquestion-ranking
 *
 * 题目描述
 * 请实现一个函数，将一个字符串中的每个空格替换成“%20”。
 * 例如，当字符串为We Are Happy.则经过替换之后的字符串为We%20Are%20Happy。
 */
public class ReplaceAllSpace extends A{
    public ReplaceAllSpace(){
        System.out.println(3);
        a = 10;
    }
    static {
        System.out.println(4);

    }
    public static void main(String[] args){
        new ReplaceAllSpace();
    }
    public String replaceSpace(StringBuffer str) {

        StringBuilder sb = new StringBuilder();
        char ch = 0;
        int len = str.length();
        for(int i = 0 ; i < len ; i++ ){
            ch = str.charAt(i);
            sb.append( ch == ' ' ? "%20" : ch );
        }

        return sb.toString();
    }
}
class A{
    static int a;
    static {
        System.out.println(1);
        System.out.println(a);
    }
    public A(){
        System.out.println(2);
        System.out.println(a);
    }
}