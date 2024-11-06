public class NativeLibrary {
    static {
        System.loadLibrary("native");
    }

    public native String hello();

    public static void main(String[] args) {
        NativeLibrary lib = new NativeLibrary();
        System.out.println(lib.hello());
    }
}