/**
 * locate PACKAGE_NAME
 * Created by 79875 on 2017/10/18.
 */
public class ThreadTest{
    public static void main(String[] args) {
        final ThreadDemo threadDemo=new ThreadDemo();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true)
                    threadDemo.A();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true)
                    threadDemo.B();
            }
        }).start();
    }
}


class ThreadDemo{
    public ThreadDemo() {
    }

    synchronized public void A(){
            System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    }
    synchronized public void B(){
            System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
        }

}
