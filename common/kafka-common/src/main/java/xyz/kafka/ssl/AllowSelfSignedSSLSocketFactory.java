package xyz.kafka.ssl;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * AllowSelfSignedSSLSocketFactory
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2024-10-30
 */
public class AllowSelfSignedSSLSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory socketFactory;

    public AllowSelfSignedSSLSocketFactory() {
        try {
            SSLContext context = SSLContext.getInstance("SSL");
            context.init(null, new TrustManager[]{new LTSTrustmanager(false)}, new SecureRandom());
            socketFactory = context.getSocketFactory();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static SocketFactory getDefault() {
        return new AllowSelfSignedSSLSocketFactory();
    }

    @Override
    public Socket createSocket(Socket s, String host,
                               int port, boolean autoClose) throws IOException {
        return makeSocketSafe(s);
    }

    private Socket makeSocketSafe(Socket socket) {
//        if (socket instanceof SSLSocket sslSocket) {
//            socket = new NoSSLv3SSLSocket(sslSocket);
//        }
        return socket;
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return socketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return socketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return socketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        return socketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        return socketFactory.createSocket(host, port, localHost, localPort);
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        return socketFactory.createSocket(address, port, localAddress, localPort);
    }

    private record LTSTrustmanager(boolean checkServerValidity) implements X509TrustManager {
        @Override
        public void checkClientTrusted(final X509Certificate[] certificates, final String authType) {
            // document why this method is empty
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] certificates, final String authType) throws CertificateException {
            if (this.checkServerValidity) {
                for (X509Certificate certificate : certificates) {
                    certificate.checkValidity();
                }
            }

        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
