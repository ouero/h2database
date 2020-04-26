package org.h2.tools;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class CompressFile {
    private static ByteArrayOutputStream tempBuffer;
    private static DataOutputStream tempOut;
    private static int messageType;
    private static FileOutputStream dataFileOutputStream;
    private static DataOutputStream indexOut;
    private static CSVReader reader;

    public static void main(String[] args) throws IOException, CsvValidationException {
        String indexName = args[0].replace(".csv", ".index");
        String dataName = args[0].replace(".csv", ".data");

        try {
            dataFileOutputStream = new FileOutputStream(dataName);

            FileOutputStream indexFile = new FileOutputStream(indexName);
            indexOut = new DataOutputStream(indexFile);

            reader = new CSVReader(new FileReader(args[0]));
            String[] nextLine = reader.readNext();

            tempBuffer = new ByteArrayOutputStream();
            tempOut = new DataOutputStream(tempBuffer);
            int x = 0;
            while (nextLine != null) {
                startMessage('D');
                int columns = nextLine.length;
                tempOut.writeShort((short) columns);
                for (int i = 1; i <= columns; i++) {
                    writeDataColumn(nextLine, i);
                }
                short length = sendMessage();
                indexOut.writeShort(length);
                nextLine = reader.readNext();
                if (++x % 10000 == 0) {
                    System.out.println("complete " + x);
                }
            }
            indexOut.flush();
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (indexOut != null) {
                indexOut.close();
            }
            if (dataFileOutputStream != null) {
                dataFileOutputStream.close();
            }
        }


    }

    public static void writeDataColumn(String[] nextline, int column) throws IOException {
        String v = nextline[column - 1];
        if (v == null || v.isEmpty()) {
            tempOut.writeInt(-1);
            return;
        }
        byte[] data = v.getBytes(StandardCharsets.UTF_8);
        tempOut.writeInt(data.length);
        tempOut.write(data);
    }

    public static void startMessage(int newMessageType) {
        messageType = newMessageType;
        tempBuffer = new ByteArrayOutputStream();
        tempOut = new DataOutputStream(tempBuffer);
    }

    public static short sendMessage() throws IOException {
        tempOut.flush();
        byte[] buff = tempBuffer.toByteArray();
        int len = buff.length;
        tempOut = new DataOutputStream(dataFileOutputStream);
        tempOut.write(messageType);
        tempOut.writeInt(len + 4);
        tempOut.write(buff);
        tempOut.flush();
        return (short) ((5 + len) - 32768);
    }
}
