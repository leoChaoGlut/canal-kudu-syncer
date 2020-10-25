package personal.leo.cks.server.exception;

import java.util.Objects;

public class FatalException extends RuntimeException {

    public FatalException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalException(String message) {
        super(message);
    }

    public static void throwIfFatal(Throwable t) {
        if (t == null) {
            return;
        }
        final boolean isFatalException = Objects.equals(FatalException.class.getSimpleName(), t.getClass().getSimpleName());
        if (isFatalException) {
            throw (FatalException) t;
        }
    }


}
