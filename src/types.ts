import { BroadcastFlags } from 'socket.io-adapter';

export enum AmqpAdapterEvent {
    Broadcast = 'BROADCAST',
    JoinRoom = 'JOIN_ROOM',
    LeaveRoom = 'LEAVE_ROOM',
    Disconnect = 'Disconnect',
    EmitServer = 'EMIT_SERVER',
    EmitServerResponse = 'EMIT_SERVER_RESPONSE',
}

/**
 * The types of requests are duplicated here because typescript does not support nested discriminated unions yet.
 * TODO: Remove this duplication once it is supported.
 * @see https://github.com/microsoft/TypeScript/issues/42384
 */
export type AmqpAdapterRequest = {
    id?: string;
    type: AmqpAdapterEvent;
    headers: {
        type: AmqpAdapterEvent;
        socketId?: string;
        options: {
            namespace: string;
            rooms: string[];
            except?: string[];
            flags?: BroadcastFlags;
        };
    };
    nodeId: string;
    packet: any;
};

export type AmqpAdapterOptions = {
    amqpUrl: string;
    requestsTimeout?: number;
    queuePrefix?: string;
    fanoutExchangeName?: string;
    topicExchangeName?: string;
};

export type ServerResponse = {
    requestId: string;
    data: any;
}

export type PendingServerRequest = {
    id: string;
    responses: ServerResponse[];
}
