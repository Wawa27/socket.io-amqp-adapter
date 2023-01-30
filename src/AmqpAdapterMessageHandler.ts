import { ConsumeMessage } from 'amqplib';
import debug from 'debug';
import { Namespace } from 'socket.io';
import { BroadcastOptions } from 'socket.io-adapter';
import AmqpAdapter from './AmqpAdapter';
import { AmqpAdapterEvent, AmqpAdapterRequest, PendingServerRequest } from './types';

/**
 * Message handler for AMQP.
 */
export default class AmqpAdapterMessageHandler {
  static readonly #debug = debug('socket.io-amqp-adapter:MessageHandler');

  #adapter: AmqpAdapter;
  #namespace: Namespace;

  /**
   * A list of pending requests that are waiting for a response
   */
  pendingRequests: Map<string, PendingServerRequest>;

  public constructor(amqpAdapter: AmqpAdapter) {
    this.#adapter = amqpAdapter;
    this.#namespace = amqpAdapter.nsp;
    this.pendingRequests = new Map();
  }

  /**
   * Message handler for the amqp channel
   * @param consumeMessage
   * @protected
   */
  public async onMessage(
    consumeMessage?: ConsumeMessage | null,
  ): Promise<void> {
    const content = JSON.parse(
      consumeMessage?.content.toString() ?? '',
    ) as AmqpAdapterRequest;

    if (content.headers.options.namespace !== this.#namespace.name) {
      return AmqpAdapterMessageHandler.#debug(
        `Ignoring request ${content.type} for different namespace ${content.headers.options.namespace}`,
      );
    }

    switch (content.type) {
      case AmqpAdapterEvent.Broadcast:
        return this.onBroadcast(content);
      case AmqpAdapterEvent.JoinRoom:
        return this.onJoinRooms(content);
      case AmqpAdapterEvent.LeaveRoom:
        return this.onLeaveRoom(content);
      case AmqpAdapterEvent.EmitServer:
        return this.onServerSideEmit(content);
      case AmqpAdapterEvent.EmitServerResponse:
        return this.onServerSideEmitResponse(content);
    }
  }

  /**
   * Called when a broadcast message is received
   * @param content
   * @protected
   */
  protected onBroadcast(content: AmqpAdapterRequest): Promise<void> {
    const options: BroadcastOptions = {
      rooms: new Set(content.headers.options.rooms),
      except: new Set(content.headers.options.except),
      flags: { ...content.headers.options.flags, local: true },
    };

    return this.#adapter.broadcast(content.packet, options);
  }

  /**
   * Makes the socket with the given id to join the specified rooms
   * @param request
   * @protected
   */
  protected async onJoinRooms(request: AmqpAdapterRequest): Promise<void> {
    // If not specified, make all sockets join the room
    if (!request.headers.socketId) {
      return this.#namespace.sockets.forEach((socket) => {
        socket.join(request.headers.options.rooms);
      });
    }

    let socket = this.#namespace.sockets.get(request.headers.socketId);
    if (!socket) {
      return AmqpAdapterMessageHandler.#debug(
        `Ignoring join room ${request.headers.options.rooms} for unknown socket ${request.headers.socketId}`,
      );
    }

    for (const room of request.headers.options.rooms) {
      await this.#adapter.amqpChannel.assertQueue(this.#adapter.queuePrefixName);
      await this.#adapter.amqpChannel.bindQueue(
        this.#adapter.queuePrefixName,
        this.#adapter.fanoutExchangeName,
        room,
      );
      await this.#adapter.amqpChannel.consume(
        this.#adapter.queuePrefixName,
        this.onMessage.bind(this),
      );

      socket!.join(room);
    }
  }

  /**
   * Makes the socket with the given id to leave the specified rooms
   * @param request
   * @protected
   */
  protected onLeaveRoom(request: AmqpAdapterRequest): void {
    if (!request.headers.socketId) {
      return this.#namespace.sockets.forEach((socket) => {
        request.headers.options.rooms.forEach((room) => {
          socket.leave(room);
        });
      });
    }

    let socket = this.#namespace.sockets.get(request.headers.socketId);
    if (!socket) {
      AmqpAdapterMessageHandler.#debug('Ignoring leave room for unknown socket', request.headers.socketId);
      return;
    }

    request.headers.options.rooms.forEach((room) => {
      socket.leave(room);
    });
  }

  /**
   * Called when a server-side emit is received
   * @param request
   * @protected
   */
  protected onServerSideEmit(request: AmqpAdapterRequest): void {
    AmqpAdapterMessageHandler.#debug('Emitting server-side packet', request.packet);

    if (!request.id) {
      return this.#namespace._onServerSideEmit(request.packet);
    }

    let isAlreadyCalled = false;
    // Callback used to send the response back to the server that called the emitServer method
    const responseCallback = (response: any) => {
      if (isAlreadyCalled) {
        AmqpAdapterMessageHandler.#debug(`Callback for request ${request.id} already called, ignoring...`);
        return;
      }
      isAlreadyCalled = true;
      this.#adapter.publish({
        type: AmqpAdapterEvent.EmitServerResponse,
        id: request.id,
        headers: {
          type: AmqpAdapterEvent.EmitServerResponse,
          options: {
            namespace: request.headers.options.namespace,
            rooms: request.headers.options.rooms,
          }
        },
        nodeId: this.#adapter.nodeId,
        packet: response,
      });
    };

    request.packet.push(responseCallback);
    this.#namespace._onServerSideEmit(request.packet);
  }

  /**
   * Called when a server-side emit response is received
   * @param request
   * @protected
   */
  protected onServerSideEmitResponse(request: AmqpAdapterRequest): void {
    if (!request.id) {
      return AmqpAdapterMessageHandler.#debug('Ignoring server-side emit response without id');
    }

    if (!this.pendingRequests.get(request.id)) {
      return AmqpAdapterMessageHandler.#debug(`Ignoring server-side emit response for unknown request ${request.id}`);
    }

    this.pendingRequests.get(request.id).responses.push({
      requestId: request.id,
      data: request.packet,
    });
  }
}
