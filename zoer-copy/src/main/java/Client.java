import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.Socket;

public class Client {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket(args[0], 9890);
        InputStream inputStream = socket.getInputStream();
        byte[] buf = new byte[1024];
        long maxSize = 0;
        int length = 0;
        //循环读取文件内容，输入流中将最多buf.length个字节的数据读入一个buf数组中,返回类型是读取到的字节数。
        //当文件读取到结尾时返回 -1,循环结束。
        while ((length = inputStream.read(buf)) != -1) {
            maxSize += length;
        }
        System.out.println(maxSize);
        //最后记得，关闭流
        inputStream.close();

    }
}
