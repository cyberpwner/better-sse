import {
	type IncomingMessage as Http1ServerRequest,
	ServerResponse as Http1ServerResponse,
} from "node:http";
import { type Http2ServerRequest, Http2ServerResponse } from "node:http2";
import { EventBuffer, type EventBufferOptions } from "./EventBuffer";
import { SseError } from "./lib/SseError";
import { type EventMap, TypedEmitter } from "./lib/TypedEmitter";
import { createPushFromIterable } from "./lib/createPushFromIterable";
import { createPushFromStream } from "./lib/createPushFromStream";
import { generateId } from "./lib/generateId";
import {
	type SanitizerFunction,
	sanitize as defaultSanitizer,
} from "./lib/sanitize";
import {
	type SerializerFunction,
	serialize as defaultSerializer,
} from "./lib/serialize";
import {
	DEFAULT_RESPONSE_CODE,
	DEFAULT_RESPONSE_HEADERS,
} from "./lib/constants";

type NodeRequest = Http1ServerRequest | Http2ServerRequest;

type NodeResponse =
	| Http1ServerResponse
	| (Http2ServerResponse & {
			write: (chunk: string) => void;
	  });

interface SessionOptions<State = DefaultSessionState>
	extends Pick<EventBufferOptions, "serializer" | "sanitizer"> {
	/**
	 * Whether to trust or ignore the last event ID given by the client in the `Last-Event-ID` request header.
	 *
	 * When set to `false`, the `lastId` property will always be initialized to an empty string.
	 *
	 * Defaults to `true`.
	 */
	trustClientEventId?: boolean;

	/**
	 * Time in milliseconds for the client to wait before attempting to reconnect if the connection is closed.
	 *
	 * This is a request to the client browser, and does not guarantee that the client will actually respect the given time.
	 *
	 * Equivalent to immediately calling `.retry().dispatch().flush()` after a connection is made.
	 *
	 * Give as `null` to avoid sending an explicit reconnection time and allow the client browser to decide itself.
	 *
	 * Defaults to `2000` milliseconds.
	 *
	 * @see https://html.spec.whatwg.org/multipage/server-sent-events.html#concept-event-stream-reconnection-time
	 */
	retry?: number | null;

	/**
	 * Time in milliseconds interval for the session to send a comment to keep the connection alive.
	 *
	 * Give as `null` to disable the connection keep-alive mechanism.
	 *
	 * Defaults to `10000` milliseconds (`10` seconds).
	 */
	keepAlive?: number | null;

	/**
	 * Status code to be sent to the client.
	 *
	 * Event stream requests can be redirected using HTTP 301 and 307 status codes.
	 * Make sure to set `Location` header when using these status codes using the `headers` property.
	 *
	 * A client can be asked to stop reconnecting by using 204 status code.
	 *
	 * Defaults to `200`.
	 */
	statusCode?: number;

	/**
	 * Additional headers to be sent along with the response.
	 */
	headers?: Record<string, string | string[]>;

	/**
	 * Custom state for this session.
	 *
	 * Use this object to safely store information related to the session and user.
	 */
	state?: State;
}

interface DefaultSessionState {
	[key: string]: unknown;
}

interface SessionEvents extends EventMap {
	connected: () => void;
	disconnected: () => void;
	push: (data: unknown, eventName: string, eventId: string) => void;
}

/**
 * A `Session` represents an open connection between the server and a client.
 *
 * It extends from the {@link https://nodejs.org/api/events.html#events_class_eventemitter | EventEmitter} class.
 *
 * It emits the `connected` event after it has connected and sent all headers to the client, and the
 * `disconnected` event after the connection has been closed.
 *
 * Note that creating a new session will immediately send the initial status code and headers to the client.
 * Attempting to write additional headers after you have created a new session will result in an error.
 *
 * @param req - The Node HTTP {@link https://nodejs.org/api/http.html#http_class_http_incomingmessage | ServerResponse} object.
 * @param res - The Node HTTP {@link https://nodejs.org/api/http.html#http_class_http_serverresponse | IncomingMessage} object.
 * @param options - Options given to the session instance.
 */
class Session<State = DefaultSessionState> extends TypedEmitter<SessionEvents> {
	/**
	 * The last event ID sent to the client.
	 *
	 * This is initialized to the last event ID given by the user, and otherwise is equal to the last number given to the `.id` method.
	 *
	 * For security reasons, keep in mind that the client can provide *any* initial ID here. Use the `trustClientEventId` constructor option to ignore the client-given initial ID.
	 *
	 * @readonly
	 */
	lastId = "";

	/**
	 * Indicates whether the session and underlying connection is open or not.
	 *
	 * @readonly
	 */
	isConnected = false;

	/**
	 * Custom state for this session.
	 *
	 * Use this object to safely store information related to the session and user.
	 *
	 * Use [module augmentation and declaration merging](https://www.typescriptlang.org/docs/handbook/declaration-merging.html#module-augmentation)
	 * to safely add new properties to the `DefaultSessionState` interface.
	 */
	state = {} as State;

	private buffer: EventBuffer;
	private request: Request;
	private response: Response;
	private res?: NodeResponse;
	private url: URL;
	private writer: WritableStreamDefaultWriter;
	private encoder = new TextEncoder();
	private serialize: SerializerFunction;
	private sanitize: SanitizerFunction;
	private initialRetry: number | null;
	private keepAliveInterval: number | null;
	private keepAliveTimer?: ReturnType<typeof setInterval>;

	constructor(
		req: Http1ServerRequest | Http2ServerRequest | Request,
		res?: Http1ServerRequest | Http2ServerResponse | Response,
		options: SessionOptions<State> = {}
	) {
		super();

		const { readable, writable } = new TransformStream();

		this.writer = writable.getWriter();

		if (req instanceof Request) {
			this.request = req;

			this.response = new Response(readable, {
				status:
					options.statusCode ??
					(res as Response | undefined)?.status ??
					DEFAULT_RESPONSE_CODE,
				headers: {
					...DEFAULT_RESPONSE_HEADERS,
					...(res ? Object.fromEntries((res as Response).headers) : {}),
					...options.headers,
				},
			});
		} else {
			if (
				!(
					res instanceof Http1ServerResponse ||
					res instanceof Http2ServerResponse
				)
			) {
				throw new SseError(
					"When providing a Node IncomingMessage or Http2ServerRequest object, a corresponding ServerResponse or Http2ServerResponse object must also be provided."
				);
			}

			this.res = res;

			const controller = new AbortController();

			req.once("close", controller.abort);
			res.once("close", controller.abort);

			const url = `http://${req.headers.host}${req.url}`;

			this.request = new Request(url, {
				duplex: "half",
				method: req.method,
				headers: req.headers as Record<string, string | string[]>,
				signal: controller.signal,
			});

			this.response = new Response(readable, {
				status: options.statusCode ?? res.statusCode ?? DEFAULT_RESPONSE_CODE,
				headers: {
					...DEFAULT_RESPONSE_HEADERS,
					...(res.getHeaders() as Record<string, string | string[]>),
					...options.headers,
				},
			});
		}

		this.url = new URL(this.request.url);

		if (options.trustClientEventId) {
			this.lastId =
				this.request.headers.get("last-event-id") ??
				this.url.searchParams.get("lastEventId") ??
				this.url.searchParams.get("evs_last_event_id") ??
				"";
		}

		if (options.state) {
			this.state = options.state;
		}

		this.initialRetry = options.retry === null ? null : (options.retry ?? 2000);

		this.keepAliveInterval =
			options.keepAlive === null ? null : (options.keepAlive ?? 10000);

		this.serialize = options.serializer ?? defaultSerializer;
		this.sanitize = options.sanitizer ?? defaultSanitizer;

		this.buffer = new EventBuffer({
			serializer: this.serialize,
			sanitizer: this.sanitize,
		});

		this.request.signal.addEventListener("abort", this.onDisconnected);

		setImmediate(this.initialize);
	}

	private initialize = async () => {
		console.log("entered init");

		if (this.res) {
			this.res.writeHead(
				this.response.status,
				Object.fromEntries(this.response.headers)
			);
		}

		if (this.url.searchParams.has("padding")) {
			this.buffer.comment(" ".repeat(2049)).dispatch();
		}

		if (this.url.searchParams.has("evs_preamble")) {
			this.buffer.comment(" ".repeat(2056)).dispatch();
		}

		if (this.initialRetry !== null) {
			this.buffer.retry(this.initialRetry).dispatch();
		}

		console.log("about to flush");

		await this.flush();

		if (this.keepAliveInterval !== null) {
			this.keepAliveTimer = setInterval(this.keepAlive, this.keepAliveInterval);
		}

		this.isConnected = true;

		this.emit("connected");
	};

	private onDisconnected = async () => {
		console.log("dc, removing abort");

		this.request.signal.removeEventListener("abort", this.onDisconnected);

		await this.writer.close();

		if (this.keepAliveTimer) {
			clearInterval(this.keepAliveTimer);
		}

		this.isConnected = false;

		this.emit("disconnected");
	};

	private keepAlive = async () => {
		this.buffer.comment().dispatch();
		await this.flush();
	};

	getRequest = () => this.request;

	getResponse = () => {
		return this.response;
	};

	/**
	 * @deprecated see https://github.com/MatthewWid/better-sse/issues/52
	 */
	event(type: string): this {
		this.buffer.event(type);

		return this;
	}

	/**
	 * @deprecated see https://github.com/MatthewWid/better-sse/issues/52
	 */
	data = (data: unknown): this => {
		this.buffer.data(data);

		return this;
	};

	/**
	 * @deprecated see https://github.com/MatthewWid/better-sse/issues/52
	 */
	id = (id = ""): this => {
		this.buffer.id(id);

		this.lastId = id;

		return this;
	};

	/**
	 * @deprecated see https://github.com/MatthewWid/better-sse/issues/52
	 */
	retry = (time: number): this => {
		this.buffer.retry(time);

		return this;
	};

	/**
	 * @deprecated see https://github.com/MatthewWid/better-sse/issues/52
	 */
	comment = (text?: string): this => {
		this.buffer.comment(text);

		return this;
	};

	/**
	 * @deprecated see https://github.com/MatthewWid/better-sse/issues/52
	 */
	dispatch = (): this => {
		this.buffer.dispatch();

		return this;
	};

	/**
	 * Flush the contents of the internal buffer to the client and clear the buffer.
	 *
	 * @deprecated see https://github.com/MatthewWid/better-sse/issues/52
	 */
	flush = async () => {
		const contents = this.buffer.read();

		console.log(`About to encode: ${contents}`);

		const encoded = this.encoder.encode(contents);

		console.log(`Encoded: ${encoded}`);

		this.buffer.clear();

		console.log("About to write");

		await this.writer.ready;

		console.log("Writer is ready");

		await this.writer.write(encoded);

		console.log("Writer has written");
	};

	/**
	 * Push an event to the client.
	 *
	 * If no event name is given, the event name is set to `"message"`.
	 *
	 * If no event ID is given, the event ID (and thus the `lastId` property) is set to a unique string generated using a cryptographic pseudorandom number generator.
	 *
	 * Emits the `push` event with the given data, event name and event ID in that order.
	 *
	 * @param data - Data to write.
	 * @param eventName - Event name to write.
	 * @param eventId - Event ID to write.
	 */
	push = async (
		data: unknown,
		eventName = "message",
		eventId = generateId()
	): Promise<void> => {
		if (!this.isConnected) {
			throw new SseError(
				"Cannot push data to a non-active session. Ensure the session is connected before attempting to push events."
			);
		}

		this.buffer.push(data, eventName, eventId);

		await this.flush();

		this.lastId = eventId;

		this.emit("push", data, eventName, eventId);
	};

	/**
	 * Pipe readable stream data as a series of events to the client.
	 *
	 * This uses the `push` method under the hood.
	 *
	 * If no event name is given in the `options` object, the event name is set to `"stream"`.
	 *
	 * @param stream - Readable stream to consume data from.
	 * @param options - Options to alter how the stream is flushed to the client.
	 *
	 * @returns A promise that resolves or rejects based on the success of the stream write finishing.
	 */
	stream = createPushFromStream(this.push);

	/**
	 * Iterate over an iterable and send yielded values as events to the client.
	 *
	 * This uses the `push` method under the hood.
	 *
	 * If no event name is given in the `options` object, the event name is set to `"iteration"`.
	 *
	 * @param iterable - Iterable to consume data from.
	 *
	 * @returns A promise that resolves once all data has been successfully yielded from the iterable.
	 */
	iterate = createPushFromIterable(this.push);

	/**
	 * Batch and send multiple events at once.
	 *
	 * If given an `EventBuffer` instance, its contents will be sent to the client.
	 *
	 * If given a callback, it will be passed an instance of `EventBuffer` which uses the same serializer and sanitizer as the session.
	 * Once its execution completes - or once it resolves if it returns a promise - the contents of the passed `EventBuffer` will be sent to the client.
	 *
	 * @param batcher - Event buffer to get contents from, or callback that takes an event buffer to write to.
	 *
	 * @returns A promise that resolves once all data from the event buffer has been successfully sent to the client.
	 *
	 * @see EventBuffer
	 */
	batch = async (
		batcher: EventBuffer | ((buffer: EventBuffer) => void | Promise<void>)
	) => {
		if (batcher instanceof EventBuffer) {
			// this.res.write(batcher.read());
		} else {
			const buffer = new EventBuffer({
				serializer: this.serialize,
				sanitizer: this.sanitize,
			});

			await batcher(buffer);

			// this.res.write(buffer.read());
		}
	};
}

export type { SessionOptions, SessionEvents, DefaultSessionState };
export { Session };
