import { Namespace } from "socket.io";
import AmqpAdapter from "./AmqpAdapter";
import { AmqpAdapterOptions } from "./types";

const createAdapter = (
  amqpOptions: AmqpAdapterOptions
): ReturnType<typeof createAdapter> => {
  return function (namespace: Namespace) {
    return new AmqpAdapter(namespace, amqpOptions);
  };
};

export default createAdapter;
