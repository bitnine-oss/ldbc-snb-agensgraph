package net.bitnine.ldbcimpl.excpetions;

/**
 * Created by ktlee on 16. 10. 11.
 */
public class AGClientException extends RuntimeException {

    public AGClientException() {
        super();
    }

    public AGClientException(String message) {
        super(message);
    }

    public AGClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public AGClientException(Throwable cause) {
        super(cause);
    }
}
