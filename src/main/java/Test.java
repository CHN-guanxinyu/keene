import javafx.beans.property.adapter.ReadOnlyJavaBeanBooleanProperty;

public class Test extends A{
    public static void main(String[] args){
        String c = "123" + "123";
    }

    int foo(){
        int c = this.hashCode();
        int b = super.bar();
        return b+c;
    }
}
class A{
    int bar(){return 1;}
}