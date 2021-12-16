package com.redis.postgredis.statements;

public final class JdbcConstants {
	/**
	 * Special value that is used to indicate that a statement returned a
	 * {@link ResultSet}. The method {@link Statement#getUpdateCount()} will return
	 * this value if the previous statement that was executed with
	 * {@link Statement#execute(String)} returned a {@link ResultSet}.
	 */
	public static final int STATEMENT_RESULT_SET = -1;
	/**
	 * Special value that is used to indicate that a statement had no result. The
	 * method {@link Statement#getUpdateCount()} will return this value if the
	 * previous statement that was executed with {@link Statement#execute(String)}
	 * returned
	 * {@link com.google.cloud.spanner.connection.StatementResult.ResultType#NO_RESULT},
	 * such as DDL statements.
	 */
	public static final int STATEMENT_NO_RESULT = -2;

	/** No instantiation */
	private JdbcConstants() {
	}
}