import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Main {
    public static void main(String[] args) throws IOException {
        InetAddress address=InetAddress.getByName(args[0]);
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(address,9890));
        serverSocketChannel.configureBlocking(true);
        SocketChannel channel;
        while (true) {
            channel = serverSocketChannel.accept();
            File file = new File(args[1]);
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel fileChannel = raf.getChannel();
            // 直接使用了transferTo()进行通道间的数据传输
            fileChannel.transferTo(0, fileChannel.size(), channel);
            break;
        }
        channel.close();

    }
}
