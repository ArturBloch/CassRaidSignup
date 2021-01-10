package cassandra.backend;

public class BackendException extends Exception {
	private static final long serialVersionUID = 1L;

	public BackendException(String message, Exception e) {
		super(message, e);
	}
}
