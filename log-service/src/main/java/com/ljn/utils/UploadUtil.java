package com.ljn.utils;

//import java.net.URISyntaxException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class UploadUtil {
    public static void upload(String path,String info) throws IOException {
        URL url = new URL(path);
        HttpURLConnection httpcon = (HttpURLConnection) url.openConnection();
        httpcon.setRequestMethod("POST");
        httpcon.setDoOutput(true);
        httpcon.setRequestProperty("Content-Type","application/json");
        OutputStream out = httpcon.getOutputStream();
        out.write(info.getBytes(StandardCharsets.UTF_8));
        out.flush();
        out.close();
        int code = httpcon.getResponseCode();
        System.out.println(code);
    }
}
