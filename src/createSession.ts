import type {
	IncomingMessage as Http1ServerRequest,
	ServerResponse as Http1ServerResponse,
} from "node:http";
import type { Http2ServerRequest, Http2ServerResponse } from "node:http2";
import {
	Session,
	type DefaultSessionState,
	type SessionOptions,
} from "./Session";

/**
 * Create a new session and return the session instance once it has connected.
 */
function createSession<State = DefaultSessionState>(
	req: Http1ServerRequest | Http2ServerRequest | Request,
	resOrOptions?:
		| Http1ServerResponse
		| Http2ServerResponse
		| Response
		| SessionOptions<State>,
	options?: SessionOptions<State>
): Promise<Session<State>> {
	return new Promise<Session<State>>((resolve) => {
		const session = new Session<State>(req, resOrOptions, options);

		if (req instanceof Request) {
			resolve(session);
		} else {
			session.once("connected", () => {
				resolve(session);
			});
		}
	});
}

export { createSession };
