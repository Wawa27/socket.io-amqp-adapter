# socket.io-amqp-adater

A socket.io adapter for AMQP.
It allows broadcasting messages to multiple socket.io servers.

## Features

* Rooms
* High performance using AMQP Topics
* allRooms support 
* ~~Broadcast with ack~~

## Installation

```bash
$ npm install socket.io-amqp-adapter
```

## Usage

The adapter is used by passing it to the `io` instance.
The adapter's init method **must** be called in order to work.

```js
const io = new Server();

io.adapter(createAdapter({ amqpUrl: "amqp://localhost:5672" }));

io.on('connection', function(socket){
  socket.join('room1');
});

io.listen(3000);
```
