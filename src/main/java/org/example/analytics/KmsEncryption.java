package org.example.analytics;


import com.google.cloud.storage.*;
import com.google.crypto.tink.*;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.integration.gcpkms.GcpKmsAead;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import org.apache.beam.sdk.transforms.Keys;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Optional;


public class KmsEncryption {
    public static Aead aead;
    public  static String aad;
    public static void initializeOnce() throws GeneralSecurityException, IOException {
        AeadConfig.register();
        aad = "Using google tink for encryption";

        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of("tink-poc","encrypted_keys.json");
        Blob blob = storage.get(blobId);
        String value = new String(blob.getContent());
        String masterKeyUri = "gcp-kms://projects/future-sunrise-333208/locations/us-central1/keyRings/test_key_ring_us/cryptoKeys/test_key2";
        KeysetHandle keysetHandle = KeysetHandle.read(
                JsonKeysetReader.withString(value),
                new GcpKmsClient().withDefaultCredentials().getAead(masterKeyUri));
        ByteArrayOutputStream symmetricKeyOutputStream = new ByteArrayOutputStream();
        CleartextKeysetHandle.write(keysetHandle, BinaryKeysetWriter.withOutputStream(symmetricKeyOutputStream));
        System.out.println("Base64: "+ Base64.getEncoder().encodeToString(symmetricKeyOutputStream.toByteArray()));
        aead = keysetHandle.getPrimitive(Aead.class);

    }
    public static byte[] encrypt(String plainText) throws GeneralSecurityException, IOException {
        byte[] ciphertext = aead.encrypt(plainText.getBytes(StandardCharsets.UTF_8),aad.getBytes(StandardCharsets.UTF_8));
        return ciphertext;

    }
    public static String decrypt(String ciphertext) throws GeneralSecurityException {
        byte[] decrypted = aead.decrypt(ciphertext.getBytes(StandardCharsets.UTF_8), aad.getBytes(StandardCharsets.UTF_8));
        return new String(decrypted, StandardCharsets.UTF_8);
    }
}

