package xyz.kafka.ssl;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * IgnoreClientCheckTrustManager
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2025-05-22
 */
public record IgnoreClientCheckTrustManager(boolean checkServerValidity) implements X509TrustManager {
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
