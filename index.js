const StreamDemux = require('stream-demux');
const AsyncStreamEmitter = require('async-stream-emitter');
const scBroker = require('sc-broker');
const ClientCluster = require('./clientcluster').ClientCluster;
const SCChannel = require('sc-channel');
const hash = require('sc-hasher').hash;

const scErrors = require('sc-errors');
const BrokerError = scErrors.BrokerError;
const ProcessExitError = scErrors.ProcessExitError;

function AbstractDataClient(dataClient) {
  AsyncStreamEmitter.call(this);
  this._dataClient = dataClient;
}

AbstractDataClient.prototype = Object.create(AsyncStreamEmitter.prototype);

AbstractDataClient.prototype.set = function () {
  this._dataClient.set.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.expire = function () {
  this._dataClient.expire.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.unexpire = function () {
  this._dataClient.unexpire.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.add = function () {
  this._dataClient.add.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.get = function () {
  this._dataClient.get.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.getRange = function () {
  this._dataClient.getRange.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.getAll = function () {
  this._dataClient.getAll.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.count = function () {
  this._dataClient.count.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.remove = function () {
  this._dataClient.remove.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.removeRange = function () {
  this._dataClient.removeRange.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.removeAll = function () {
  this._dataClient.removeAll.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.splice = function () {
  this._dataClient.splice.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.pop = function () {
  this._dataClient.pop.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.hasKey = function () {
  this._dataClient.hasKey.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.exec = function () {
  return this._dataClient.exec.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.extractKeys = function (object) {
  return this._dataClient.extractKeys(object);
};

AbstractDataClient.prototype.extractValues = function (object) {
  return this._dataClient.extractValues(object);
};

function SCExchange(privateClientCluster, publicClientCluster, ioClusterClient) {
  AbstractDataClient.call(this, publicClientCluster);

  this._privateClientCluster = privateClientCluster;
  this._publicClientCluster = publicClientCluster;
  this._ioClusterClient = ioClusterClient;
  this._channelMap = {};

  this._channelEventDemux = new StreamDemux();
  this._channelDataDemux = new StreamDemux();

  (async () => {
    for await (let {channel, data} of this._ioClusterClient.listener('message')) {
      this._channelDataDemux.write(channel, data);
    }
  })();
}

SCExchange.prototype = Object.create(AbstractDataClient.prototype);

SCExchange.SUBSCRIBED = SCExchange.prototype.SUBSCRIBED = SCChannel.SUBSCRIBED;
SCExchange.PENDING = SCExchange.prototype.PENDING = SCChannel.PENDING;
SCExchange.UNSUBSCRIBED = SCExchange.prototype.UNSUBSCRIBED = SCChannel.UNSUBSCRIBED;

SCExchange.prototype.destroy = function () {
  this._ioClusterClient.destroy();
};

SCExchange.prototype._triggerChannelSubscribe = function (channel) {
  let channelName = channel.name;

  if (channel.state !== SCChannel.SUBSCRIBED) {
    let oldChannelState = channel.state;
    channel.state = SCChannel.SUBSCRIBED;

    let stateChangeData = {
      oldChannelState,
      newChannelState: channel.state
    };

    this._channelEventDemux.write(`${channelName}/subscribeStateChange`, stateChangeData);
    this._channelEventDemux.write(`${channelName}/subscribe`, {});
    this.emit('subscribe', {channel: channelName});
  }
};

SCExchange.prototype._triggerChannelSubscribeFail = function (err, channel) {
  let channelName = channel.name;

  delete this._channelMap[channelName];
  this._channelEventDemux.write(`${channelName}/subscribeFail`, {error: err});
  this.emit('subscribeFail', {error: err, channel: channelName});
};

SCExchange.prototype._triggerChannelUnsubscribe = function (channel) {
  let channelName = channel.name;

  delete this._channelMap[channelName];
  if (channel.state === SCChannel.SUBSCRIBED) {
    let stateChangeData = {
      oldChannelState: channel.state,
      newChannelState: SCChannel.UNSUBSCRIBED
    };
    this._channelEventDemux.write(`${channelName}/subscribeStateChange`, stateChangeData);
    this._channelEventDemux.write(`${channelName}/unsubscribe`, {});
    this.emit('unsubscribe', {channel: channelName});
  }
};

SCExchange.prototype.sendRequest = function (data, mapIndex) {
  if (mapIndex == null) {
    // Send to all brokers in cluster if mapIndex is not provided
    mapIndex = '*';
  }
  let targetClients = this._privateClientCluster.map({mapIndex: mapIndex}, 'sendRequest');

  let sendToClientsPromises = targetClients.map((client) => {
    return client.sendRequest(data);
  });
  if (typeof mapIndex === 'number') {
    return sendToClientsPromises[0];
  }
  return Promise.all(sendToClientsPromises);
};

SCExchange.prototype.sendMessage = function (data, mapIndex) {
  if (mapIndex == null) {
    // Send to all brokers in cluster if mapIndex is not provided
    mapIndex = '*';
  }
  let targetClients = this._privateClientCluster.map({mapIndex: mapIndex}, 'sendMessage');

  targetClients.forEach((client) => {
    client.sendMessage(data);
  });
  return Promise.resolve();
};

SCExchange.prototype.publish = function (channelName, data) {
  return this._ioClusterClient.publish(channelName, data);
};

SCExchange.prototype.subscribe = function (channelName) {
  let channel = this._channelMap[channelName];

  if (!channel) {
    channel = {
      name: channelName,
      state: SCChannel.PENDING
    };
    this._channelMap[channelName] = channel;
    (async () => {
      try {
        await this._ioClusterClient.subscribe(channelName)
      } catch (err) {
        this._triggerChannelSubscribeFail(err, channel);
        return;
      }
      this._triggerChannelSubscribe(channel);
    })();
  }

  let channelDataStream = this._channelDataDemux.stream(channelName);
  let channelIterable = new SCChannel(
    channelName,
    this,
    this._channelEventDemux,
    channelDataStream
  );

  return channelIterable;
};

SCExchange.prototype.unsubscribe = function (channelName) {
  let channel = this._channelMap[channelName];

  if (channel) {
    this._triggerChannelUnsubscribe(channel);

    // The only case in which unsubscribe can fail is if the connection is closed or dies.
    // If that's the case, the server will automatically unsubscribe the client so
    // we don't need to check for failure since this operation can never fail.

    this._ioClusterClient.unsubscribe(channelName);
  }
};

SCExchange.prototype.channel = function (channelName) {
  let currentChannel = this._channelMap[channelName];

  let channelDataStream = this._channelDataDemux.stream(channelName);
  let channelIterable = new SCChannel(
    channelName,
    this,
    this._channelEventDemux,
    channelDataStream
  );

  return channelIterable;
};

SCExchange.prototype.getChannelState = function (channelName) {
  let channel = this._channelMap[channelName];
  if (channel) {
    return channel.state;
  }
  return SCChannel.UNSUBSCRIBED;
};

SCExchange.prototype.getChannelOptions = function (channelName) {
  return {};
};

SCExchange.prototype.closeChannel = function (channelName) {
  this._channelDataDemux.close(channelName);
};

SCExchange.prototype.subscriptions = function (includePending) {
  let subs = [];
  Object.keys(this._channelMap).forEach((channelName) => {
    if (includePending || this._channelMap[channelName].state === SCChannel.SUBSCRIBED) {
      subs.push(channelName);
    }
  });
  return subs;
};

SCExchange.prototype.isSubscribed = function (channelName, includePending) {
  let channel = this._channelMap[channelName];
  if (includePending) {
    return !!channel;
  }
  return !!channel && channel.state === SCChannel.SUBSCRIBED;
};

SCExchange.prototype.setMapper = function (mapper) {
  this._publicClientCluster.setMapper(mapper);
};

SCExchange.prototype.getMapper = function () {
  return this._publicClientCluster.getMapper();
};

SCExchange.prototype.map = function () {
  return this._publicClientCluster.map.apply(this._publicClientCluster, arguments);
};

function Server(options) {
  AsyncStreamEmitter.call(this);

  this._dataServers = [];
  this.isShuttingDown = false;

  let len = options.brokers.length;
  let startDebugPort = options.debug;
  let startInspectPort = options.inspect;

  let serverReadyPromises = [];

  for (let i = 0; i < len; i++) {
    let launchServer = async (i, isRespawn) => {
      let socketPath = options.brokers[i];

      let dataServer = scBroker.createServer({
        brokerId: i,
        debug: startDebugPort ? startDebugPort + i : null,
        inspect: startInspectPort ? startInspectPort + i : null,
        instanceId: options.instanceId,
        socketPath: socketPath,
        secretKey: options.secretKey,
        expiryAccuracy: options.expiryAccuracy,
        downgradeToUser: options.downgradeToUser,
        brokerControllerPath: options.appBrokerControllerPath,
        processTermTimeout: options.processTermTimeout,
        ipcAckTimeout: options.ipcAckTimeout,
        brokerOptions: options.brokerOptions
      });

      this._dataServers[i] = dataServer;

      (async () => {
        for await (let event of dataServer.listener('error')) {
          this.emit('error', event);
        }
      })();

      (async () => {
        for await (let event of dataServer.listener('exit')) {
          let exitMessage = `Broker server at socket path ${socketPath} exited with code ${event.code}`;
          if (event.signal != null) {
            exitMessage += ` and signal ${event.signal}`;
          }
          let error = new ProcessExitError(exitMessage, event.code);
          error.pid = process.pid;
          if (event.signal != null) {
            error.signal = event.signal;
          }
          this.emit('error', {error});
          this.emit('brokerExit', {
            brokerId: event.brokerId,
            pid: event.pid,
            code: event.code,
            signal: event.signal
          });
          if (!this.isShuttingDown) {
            launchServer(i, true);
          }
        }
      })();

      (async () => {
        for await (let event of dataServer.listener('brokerRequest')) {
          this.emit('brokerRequest', event);
        }
      })();

      (async () => {
        let brokerId = dataServer.options.brokerId;
        for await (let {data} of dataServer.listener('brokerMessage')) {
          this.emit('brokerMessage', {
            brokerId,
            data
          });
        }
      })();

      let brokerInfo = await dataServer.listener('ready').once();
      this.emit('brokerStart', {
        brokerId: brokerInfo.brokerId,
        pid: brokerInfo.pid,
        respawn: !!isRespawn
      });
    };

    serverReadyPromises.push(launchServer(i));
  }
  (async () => {
    await Promise.all(serverReadyPromises);
    this.emit('ready', {});
  })();
}

Server.prototype = Object.create(AsyncStreamEmitter.prototype);

Server.prototype.sendRequestToBroker = function (brokerId, data) {
  let targetBroker = this._dataServers[brokerId];
  if (targetBroker) {
    return targetBroker.sendRequestToBroker(data);
  }
  let error = new BrokerError(`Broker with id ${brokerId} does not exist`);
  error.pid = process.pid;
  this.emit('error', {error});
  return Promise.reject(error);
};

Server.prototype.sendMessageToBroker = function (brokerId, data) {
  let targetBroker = this._dataServers[brokerId];
  if (targetBroker) {
    return targetBroker.sendMessageToBroker(data);
  }
  let error = new BrokerError(`Broker with id ${brokerId} does not exist`);
  error.pid = process.pid;
  this.emit('error', {error});
  return Promise.reject(error);
};

Server.prototype.killBrokers = function () {
  this._dataServers.forEach((dataServer) => {
    dataServer.destroy();
  });
};

Server.prototype.destroy = function () {
  this.isShuttingDown = true;
  this.killBrokers();
};

function Client(options) {
  AsyncStreamEmitter.call(this);

  this.options = options;
  this.isReady = false;

  let dataClients = [];

  options.brokers.forEach((socketPath) => {
    let dataClient = scBroker.createClient({
      socketPath: socketPath,
      ...options
    });
    dataClients.push(dataClient);
  });

  let hasher = (key) => {
    return hash(key, dataClients.length);
  };

  let channelMethods = {
    publish: true,
    subscribe: true,
    unsubscribe: true
  };

  this._defaultMapper = (key, method, clientIds) => {
    if (channelMethods[method]) {
      if (key == null) {
        return clientIds;
      }
      return hasher(key);
    } else if (
      method === 'query' ||
      method === 'exec' ||
      method === 'sendRequest' ||
      method === 'sendMessage'
    ) {
      let mapIndex = key.mapIndex;
      if (mapIndex) {
        // A mapIndex of * means that the action should be sent to all
        // brokers in the cluster.
        if (mapIndex === '*') {
          return clientIds;
        } else {
          if (Array.isArray(mapIndex)) {
            let hashedIndexes = [];
            let len = mapIndex.length;
            for (let i = 0; i < len; i++) {
              hashedIndexes.push(hasher(mapIndex[i]));
            }
            return hashedIndexes;
          } else {
            return hasher(mapIndex);
          }
        }
      }
      return 0;
    } else if (method === 'removeAll') {
      return clientIds;
    }
    return hasher(key);
  };

  let emitError = (event) => {
    this.emit('error', event);
  };
  let emitWarning = (event) => {
    this.emit('warning', event);
  };

  // The user cannot change the _defaultMapper for _privateClientCluster.
  this._privateClientCluster = new ClientCluster(dataClients);
  this._privateClientCluster.setMapper(this._defaultMapper);
  (async () => {
    for await (let event of this._privateClientCluster.listener('error')) {
      emitError(event);
    }
  })();
  (async () => {
    for await (let event of this._privateClientCluster.listener('warning')) {
      emitWarning(event);
    }
  })();

  // The user can provide a custom mapper for _publicClientCluster.
  // The _defaultMapper is used by default.
  this._publicClientCluster = new ClientCluster(dataClients);
  this._publicClientCluster.setMapper(this._defaultMapper);
  (async () => {
    for await (let event of this._publicClientCluster.listener('error')) {
      emitError(event);
    }
  })();
  (async () => {
    for await (let event of this._publicClientCluster.listener('warning')) {
      emitWarning(event);
    }
  })();

  this._sockets = {};

  this._exchangeSubscriptions = {};
  this._exchangeClient = new SCExchange(this._privateClientCluster, this._publicClientCluster, this);

  this._clientSubscribers = {};
  this._clientSubscribersCounter = {};

  (async () => {
    await Promise.all(
      dataClients.map((dataClient) => {
        return dataClient.listener('ready').once();
      })
    );
    this.isReady = true;
    this.emit('ready', {});
  })();

  (async () => {
    for await (let event of this._privateClientCluster.listener('message')) {
      this._handleExchangeMessage(event);
    }
  })();
}

Client.prototype = Object.create(AsyncStreamEmitter.prototype);

Client.prototype.destroy = function () {
  this._privateClientCluster.destroy();
  this._publicClientCluster.destroy();
};

Client.prototype.exchange = function () {
  return this._exchangeClient;
};

Client.prototype._dropUnusedSubscriptions = function (channel) {
  let subscriberCount = this._clientSubscribersCounter[channel];
  if (subscriberCount == null || subscriberCount <= 0) {
    delete this._clientSubscribers[channel];
    delete this._clientSubscribersCounter[channel];

    if (!this._exchangeSubscriptions[channel]) {
      return this._privateClientCluster.unsubscribe(channel);
    }
  }
  return Promise.resolve();
};

Client.prototype.publish = function (channelName, data) {
  return this._privateClientCluster.publish(channelName, data);
};

Client.prototype.subscribe = async function (channel) {
  if (!this._exchangeSubscriptions[channel]) {
    this._exchangeSubscriptions[channel] = 'pending';
    try {
      await this._privateClientCluster.subscribe(channel);
    } catch (err) {
      delete this._exchangeSubscriptions[channel];
      this._dropUnusedSubscriptions(channel);
      throw err;
    }
    this._exchangeSubscriptions[channel] = true;
  }
};

Client.prototype.unsubscribe = function (channel) {
  delete this._exchangeSubscriptions[channel];
  return this._dropUnusedSubscriptions(channel);
};

Client.prototype.unsubscribeAll = function () {
  let dropSubscriptionsPromises = Object.keys(this._exchangeSubscriptions)
  .map((channel) => {
    delete this._exchangeSubscriptions[channel];
    return this._dropUnusedSubscriptions(channel);
  });

  return Promise.all(dropSubscriptionsPromises);
};

Client.prototype.isSubscribed = function (channel, includePending) {
  if (includePending) {
    return !!this._exchangeSubscriptions[channel];
  }
  return this._exchangeSubscriptions[channel] === true;
};

Client.prototype.subscribeSocket = async function (socket, channel) {
  await this._privateClientCluster.subscribe(channel);
  if (!this._clientSubscribers[channel]) {
    this._clientSubscribers[channel] = {};
    this._clientSubscribersCounter[channel] = 0;
  }
  if (!this._clientSubscribers[channel][socket.id]) {
    this._clientSubscribersCounter[channel]++;
  }
  this._clientSubscribers[channel][socket.id] = socket;
};

Client.prototype.unsubscribeSocket = function (socket, channel) {
  if (this._clientSubscribers[channel]) {
    if (this._clientSubscribers[channel][socket.id]) {
      this._clientSubscribersCounter[channel]--;
      delete this._clientSubscribers[channel][socket.id];

      if (this._clientSubscribersCounter[channel] <= 0) {
        delete this._clientSubscribers[channel];
        delete this._clientSubscribersCounter[channel];
      }
    }
  }
  return this._dropUnusedSubscriptions(channel);
};

Client.prototype.setSCServer = function (scServer) {
  this.scServer = scServer;
};

Client.prototype._handleExchangeMessage = function (packet) {
  let emitOptions = {};
  if (this.scServer) {
    // Optimization
    try {
      emitOptions.stringifiedData = this.scServer.codec.encode({
        event: '#publish',
        data: packet
      });
    } catch (error) {
      this.emit('error', {error});
      return;
    }
  }

  let subscriberSockets = this._clientSubscribers[packet.channel] || {};

  Object.keys(subscriberSockets).forEach((i) => {
    subscriberSockets[i].transmit('#publish', packet, emitOptions);
  });

  this.emit('message', packet);
};

module.exports.Client = Client;
module.exports.Server = Server;
