package be.nabu.libs.http.client.websocket;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.CookieHandler;
import java.net.Socket;
import java.security.Principal;
import java.util.List;
import java.util.Random;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.ClientAuthenticationHandler;
import be.nabu.libs.http.api.server.MessageDataProvider;
import be.nabu.libs.http.client.DefaultHTTPClient;
import be.nabu.libs.http.client.connections.PlainConnectionHandler;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.http.server.websockets.WebSocketHandshakeHandler;
import be.nabu.libs.http.server.websockets.api.WebSocketMessage;
import be.nabu.libs.http.server.websockets.api.WebSocketRequest;
import be.nabu.libs.http.server.websockets.impl.WebSocketMessageFormatterFactory;
import be.nabu.libs.http.server.websockets.impl.WebSocketRequestParserFactory;
import be.nabu.libs.nio.api.MessageParser;
import be.nabu.utils.codec.TranscoderUtils;
import be.nabu.utils.codec.impl.Base64Encoder;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.PushbackContainer;
import be.nabu.utils.io.api.WritableContainer;
import be.nabu.utils.mime.api.Header;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.MimeUtils;
import be.nabu.utils.mime.impl.PlainMimeEmptyPart;

public class WebSocketClient implements Closeable {
	
	private Socket socket;
	private EventDispatcher dispatcher;
	private WebSocketRequestParserFactory parserFactory;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private WebSocketMessageFormatterFactory formatterFactory;
	private WritableContainer<ByteBuffer> output;
	private Thread thread;
	
	public static WebSocketClient connect(SSLContext context, String host, Integer port, String path, int socketTimeout, ClientAuthenticationHandler authenticationHandler, CookieHandler cookieHandler, Principal principal, List<String> protocols, MessageDataProvider dataProvider, EventDispatcher dispatcher) {
		PlainConnectionHandler plainHandler = new PlainConnectionHandler(context, 30000, socketTimeout);
		plainHandler.setCloseOnRelease(false);
		try {
			DefaultHTTPClient client = new DefaultHTTPClient(plainHandler, authenticationHandler, cookieHandler, false);
			if (port == null) {
				port = context == null ? 80 : 443;
			}
			// in all the examples i've seen they use 18 bytes, not sure if this is in the spec or not...
			byte [] bytes = new byte[18];
			new Random().nextBytes(bytes);
			// base64 encode them
			String value = new String(IOUtils.toBytes(TranscoderUtils.transcodeBytes(IOUtils.wrap(bytes, true), new Base64Encoder())), "ASCII");
			PlainMimeEmptyPart content = new PlainMimeEmptyPart(null, 
				new MimeHeader("Host", host + ":" + port),
				new MimeHeader("Upgrade", "websocket"),
				new MimeHeader("Connection", "Upgrade"),
				new MimeHeader("Sec-WebSocket-Key", value),
				new MimeHeader("Sec-WebSocket-Version", "13")
			);
			if (protocols != null && !protocols.isEmpty()) {
				StringBuilder builder = new StringBuilder();
				for (int i = 0; i < protocols.size(); i++) {
					if (i > 0) {
						builder.append(", ");
					}
					builder.append(protocols.get(i));
				}
				content.setHeader(new MimeHeader("Sec-WebSocket-Protocol", builder.toString()));
			}
			HTTPResponse response = client.execute(new DefaultHTTPRequest(
				"GET",
				path,
				content
			), principal, context != null, true);
			
			if (response.getCode() != 101) {
				throw new IOException("The server did not respond with the expected 101 code, instead we received " + response.getCode() + ": " + response.getMessage());
			}
			Header header = MimeUtils.getHeader("Sec-WebSocket-Accept", response.getContent().getHeaders());
			if (header == null) {
				throw new IOException("The response did not include an accept header");
			}
			String expected = WebSocketHandshakeHandler.calculateResponse(value);
			if (!expected.equals(header.getValue().trim())) {
				throw new IOException("The server did not respond with the expected value '" + expected + "', instead we received '" + header.getValue() + "'");
			}
			List<Socket> openSockets = plainHandler.getOpenSockets();
			if (openSockets.isEmpty()) {
				throw new IOException("The socket was closed after the upgrade request");
			}
			else if (openSockets.size() > 1) {
				throw new IOException("More than one socket was used in the upgrade request");
			}
			return new WebSocketClient(
				openSockets.get(0),
				new WebSocketRequestParserFactory(dataProvider, protocols, path, 13, null, null),
				dispatcher
			);
		}
		catch(Exception e) {
			try {
				plainHandler.close();
			}
			catch (IOException e1) {
				// ignore...
			}
			throw new RuntimeException(e);
		}
	}
	
	public WebSocketClient(Socket socket, WebSocketRequestParserFactory parserFactory, EventDispatcher dispatcher) {
		this.socket = socket;
		this.parserFactory = parserFactory;
		this.dispatcher = dispatcher;
		this.formatterFactory = new WebSocketMessageFormatterFactory(true);
	}
	
	public boolean isClosed() {
		return thread == null || !thread.isAlive();
	}
	
	public void start() {
		thread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					PushbackContainer<ByteBuffer> readable = IOUtils.pushback(IOUtils.wrap(socket.getInputStream()));
					MessageParser<WebSocketRequest> parser = null;
					while (!Thread.interrupted()) {
						if (parser == null) {
							parser = parserFactory.newMessageParser();
						}
						parser.push(readable);
						if (parser.isDone()) {
							dispatcher.fire(parser.getMessage(), WebSocketClient.this);
							if (parser.isClosed()) {
								break;
							}
							else {
								parser = null;
							}
						}
					}
				}
				catch (IOException e) {
					logger.warn("Could not process incoming websocket message", e);
				}
				catch (Exception e) {
					logger.error("Could not process incoming websocket message", e);
				}
				finally {
					try {
						logger.error("Disconnecting websocket client");
						socket.close();
					}
					catch (IOException f) {
						// ignore
					}
				}
			}
		});
		thread.setDaemon(true);
		thread.start();
	}
	
	public void send(WebSocketMessage message) throws IOException {
		if (output == null) {
			synchronized(this) {
				if (output == null) {
					output = IOUtils.wrap(new BufferedOutputStream(socket.getOutputStream()));
				}
			}
		}
		IOUtils.copyBytes(formatterFactory.newMessageFormatter().format(message), output);
		output.flush();
	}

	public WebSocketRequestParserFactory getParserFactory() {
		return parserFactory;
	}
	
	public void close() throws IOException {
		if (thread != null) {
			thread.interrupt();
		}
		socket.close();
	}
}
