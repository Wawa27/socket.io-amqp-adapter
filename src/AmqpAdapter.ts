import {
  Adapter,
  BroadcastOptions,
  Room,
  SocketId,
} from 'socket.io-adapter';
import { Channel, connect, Connection } from 'amqplib';
import { Namespace } from 'socket.io';
import debug from 'debug';
import AmqpAdapterMessageHandler from './AmqpAdapterMessageHandler';
import { v4 as uuidv4 } from 'uuid';
import { AmqpAdapterEvent, AmqpAdapterOptions, AmqpAdapterRequest } from './types';

/**
 * Adapter for socket.io that uses AMQP as transport.
 */
export default class AmqpAdapter extends Adapter {
  static readonly #debug = debug('socketio-amqp-adapter:Adapter');

  readonly #amqpOptions: AmqpAdapterOptions;
  #amqpConnection?: Connection;
  #amqpChannel?: Channel;
  readonly #requestsTimeout;
  /**
   * Amqp queue prefix
   * @private
   */
  readonly queuePrefixName: string;
  /**
   * Name for the amqp fanout exchange
   * @private
   */
  readonly #fanoutExchangeName: string;
  /**
   * Name for the amqp topic exchange
   * @private
   */
  readonly #topicExchangeName: string;
  /**
   * TODO: Remove this once the type is added to socket.io
   * The socket-io Adapter class has no type for Namespace yet, so we keep and use it here until it is added.
   * @protected
   */
  #namespace: Namespace;
  readonly #messageHandler: AmqpAdapterMessageHandler;
  readonly nodeId: string;

  public constructor(namespace: Namespace, amqpOptions: AmqpAdapterOptions) {
    super(namespace);
    this.#namespace = namespace;
    this.#amqpOptions = amqpOptions;
    this.#requestsTimeout = amqpOptions.requestsTimeout ?? 5000;
    this.nodeId = uuidv4();
    this.queuePrefixName = amqpOptions.queuePrefix ?? 'socketio-amqp-adapter-';
    this.#fanoutExchangeName = amqpOptions.fanoutExchangeName ?? 'socketio-amqp-adapter-fanout';
    this.#topicExchangeName = amqpOptions.topicExchangeName ?? 'socketio-amqp-adapter-topic';
    this.#messageHandler = new AmqpAdapterMessageHandler(this);
  }

  /**
   * Initialize the adapter, it **must** be called by the client
   */
  public async init(): Promise<void> {
    this.#amqpConnection = await connect(this.#amqpOptions.amqpUrl);

    this.#amqpChannel = await this.#amqpConnection.createChannel();
    await this.#amqpChannel.assertExchange(this.#fanoutExchangeName, 'fanout');
    let fanoutQueueName = `${this.queuePrefixName}fanout-${this.nodeId}`;
    await this.amqpChannel!.assertQueue(fanoutQueueName, {
      durable: false,
      autoDelete: true,
    });
    await this.#amqpChannel.bindQueue(fanoutQueueName, this.#fanoutExchangeName, '');
    await this.amqpChannel!.consume(
      fanoutQueueName,
      this.#messageHandler.onMessage.bind(this.#messageHandler),
    );

    await this.#amqpChannel.assertExchange(this.#topicExchangeName, 'topic');

    this.#namespace.on('listRooms', (callback) => {
      callback(Array.from(this.rooms.keys()));
    });
  }

  /**
   * Make the socket join the specified rooms
   * @param socketId
   * @param rooms
   */
  public async addAll(socketId: SocketId, rooms: Set<Room>): Promise<void> {
    AmqpAdapter.#debug(`Adding socket ${socketId} to rooms ${Array.from(rooms.keys()).join(', ')}`);
    if (!this.#namespace.sockets.has(socketId)) {
      AmqpAdapter.#debug(`Unknown socket ${socketId}`);
      return;
    }

    super.addAll(socketId, rooms);

    for (const room of rooms) {
      // Create the queue when the first socket joins the room
      if (this.rooms.get(room)?.size === 1) {
        AmqpAdapter.#debug(`New room ${room} created`);
        const queueName = `${this.queuePrefixName}topic-${this.nodeId}-${room}`;
        await this.amqpChannel!.assertQueue(queueName, {
          durable: false,
          autoDelete: true,
        });
        await this.#amqpChannel.bindQueue(queueName, this.#topicExchangeName, '*.' + room);
        await this.amqpChannel!.consume(
          queueName,
          this.#messageHandler.onMessage.bind(this.#messageHandler),
        );
      }
    }
  }

  /**
   * Make the socket leave the specified room
   * @param socketId
   * @param room
   */
  public async del(socketId: SocketId, room: Room): Promise<void> {
    AmqpAdapter.#debug(`Removing socket ${socketId} from room ${room}`);

    if (!this.#namespace.sockets.has(socketId)) {
      AmqpAdapter.#debug(`Unknown socket ${socketId}`);
      return;
    }


    super.del(socketId, room);

    // Remove the queue if there are no more sockets in the room
    if (this.rooms.get(room)?.size === 0) {
      AmqpAdapter.#debug(`Room ${room} deleted`);
      await this.#amqpChannel?.deleteQueue(`${this.queuePrefixName}topic-${this.nodeId}-${room}`);
    }
  }

  /**
   * Make the socket leave all rooms it's joined
   * @param socketId
   */
  public async delAll(socketId: SocketId): Promise<void> {
    AmqpAdapter.#debug(`Removing socket ${socketId} from all rooms`);

    if (!this.#namespace.sockets.has(socketId)) {
      AmqpAdapter.#debug(`Unknown socket ${socketId}`);
      return;
    }

    let rooms = this.rooms.get(socketId) ?? new Set();
    for (let i = rooms.size - 1; i >= 0; i--) {
      await this.del(socketId, rooms[i]);
    }
  }

  /**
   * Makes the matching socket instances join the specified rooms
   * @param options
   * @param rooms
   */
  public addSockets(options: BroadcastOptions, rooms: Room[]): void {
    AmqpAdapter.#debug('addSockets not supported yet');
    super.addSockets(options, rooms);
  }

  /**
   * Makes the matching socket instances leave the specified rooms
   * @param options
   * @param rooms
   */
  public delSockets(options: BroadcastOptions, rooms: Room[]): void {
    AmqpAdapter.#debug('delSockets not supported yet');
    super.delSockets(options, rooms);
  }

  /**
   * Broadcast a packet to all the remote sockets in the specified rooms
   * @param packet
   * @param options
   */
  public async broadcast(packet: any, options: BroadcastOptions): Promise<void> {
    if (options.flags.local) {
      return super.broadcast(packet, options);
    }
    AmqpAdapter.#debug(`Broadcasting packet ${packet} to rooms ${Array.from(options.rooms.values()).join(', ')}`);

    const content: AmqpAdapterRequest = {
      id: uuidv4(),
      type: AmqpAdapterEvent.Broadcast,
      headers: {
        type: AmqpAdapterEvent.Broadcast,
        options: {
          namespace: this.#namespace.name,
          except: Array.from(options.except ?? []),
          rooms: [],
          flags: options.flags,
        },
      },
      nodeId: this.nodeId,
      packet,
    };

    if (options.rooms.size === 0) {
      this.publish(content);
    } else {
      options.rooms.forEach((room) => {
        content.headers.options.rooms = [room];
        this.publish(content, room);
      });
    }
  }

  /**
   * Send a message to amqp
   * @param request The amqp request
   * @param room The socket.io room to send the message to, will use the fanout exchange if not specified
   * @private
   */
  public publish(request: AmqpAdapterRequest, room?: string) {
    if (room) {
      this.amqpChannel?.publish(
        this.#topicExchangeName,
        `*.${room}`,
        Buffer.from(JSON.stringify(request)),
        { expiration: this.#requestsTimeout },
      );
    } else {
      this.amqpChannel?.publish(
        this.#fanoutExchangeName,
        '',
        Buffer.from(JSON.stringify(request)),
        { expiration: this.#requestsTimeout },
      );
    }
  }

  /**
   * Similar to {@link broadcast}, with added acknowledgement support
   * @param packet The packet to broadcast
   * @param opts The broadcast options
   * @param clientCountCallback A callback called with the number of clients that received the packet (can be called several times in a cluster)
   * @param ack A callback called for each client response
   */
  public async broadcastWithAck(
    packet: any,
    opts: BroadcastOptions,
    clientCountCallback: (clientCount: number) => void,
    ack: (...args: any[]) => void,
  ): Promise<void> {
    AmqpAdapter.#debug('broadcastWithAck not supported yet');
    return super.broadcastWithAck(packet, opts, clientCountCallback, ack);
  }

  public allRooms(callback: (err: any, rooms: Set<Room>) => void): void {
    const localRooms = new Set(this.rooms.keys());

    this.serverSideEmit('listRooms', (error, responses: { rooms: string[] }[]) => {
      if (error) {
        return callback(error, null);
      }

      if (!responses) {
        return callback("No responses found for listRooms", null);
      }

      responses.forEach((response) => {
        response.rooms.forEach((room) => localRooms.add(room));
      });

      callback(null, localRooms);
    });
  }

  /**
   * Emit events to socket.io servers in the cluster
   */
  public serverSideEmit(...packet: any): void {
    AmqpAdapter.#debug('Emitting to socket.io servers in the cluster');

    const callback = typeof packet[packet.length - 1] === 'function' ? packet.pop() : undefined;

    const request: AmqpAdapterRequest = {
      id: uuidv4(),
      type: AmqpAdapterEvent.EmitServer,
      headers: {
        type: AmqpAdapterEvent.EmitServer,
        options: {
          namespace: this.#namespace.name,
          rooms: [],
        },
      },
      nodeId: this.nodeId,
      packet,
    };

    this.#messageHandler.pendingRequests.set(request.id, {
      id: request.id,
      responses: [],
    });

    if (!(callback instanceof Function)) {
      return this.publish(request);
    }

    this.publish(request);

    // TODO: Use acknowledgement api instead of timeout
    new Promise((resolve) => setTimeout(resolve, 750)).then(() => {
      const resolvedRequest = this.#messageHandler.pendingRequests.get(request.id);
      this.#messageHandler.pendingRequests.delete(request.id);

      callback(null, resolvedRequest.responses.map((response) => response.data));
    });
  }

  /**
   * Close the amqp connection
   */
  public async close(): Promise<void> {
    await this.#amqpConnection?.close();
  }

  /**
   * Get the number of socket.io servers in the cluster
   */
  public async serverCount(): Promise<number> {
    AmqpAdapter.#debug('serverCount not supported yet');
    return super.serverCount();
  }

  public get amqpChannel(): Channel {
    return this.#amqpChannel!;
  }

  public get fanoutExchangeName(): string {
    return this.#fanoutExchangeName;
  }
}
