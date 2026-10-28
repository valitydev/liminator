package dev.vality.liminator.util;

import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@UtilityClass
public class OperationLockKey {

    private static final String NAMESPACE = "liminator:operation\0";

    public static long fromOperationId(String operationId) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest((NAMESPACE + operationId).getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
