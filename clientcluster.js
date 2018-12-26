const AsyncStreamEmitter = require('async-stream-emitter');

function ClientCluster(clients) {
  AsyncStreamEmitter.call(this);

  let self = this;

  let i, method;
  let client = clients[0];
  let clientIds = [];

  let clientAsyncInterface = [
    'subscribe',
    'unsubscribe',
    'publish',
    'set',
    'getExpiry',
    'add',
    'concat',
    'get',
    'getRange',
    'getAll',
    'count',
    'registerDeathQuery',
    'exec',
    'query',
    'remove',
    'removeRange',
    'removeAll',
    'splice',
    'pop',
    'hasKey',
    'sendRequest',
    'sendMessage',
    'end'
  ];

  let clientUtils = [
    'extractKeys',
    'extractValues'
  ];

  clients.forEach((client, i) => {
    (async () => {
      for await (let event of client.listener('error')) {
        this.emit('error', event);
      }
    })();

    (async () => {
      for await (let event of client.listener('warning')) {
        this.emit('warning', event);
      }
    })();

    (async () => {
      for await (let event of client.listener('message')) {
        this.emit('message', event);
      }
    })();

    client.id = i;
    clientIds.push(i);
  });

  // Default mapper maps to all clients.
  let mapper = function () {
    return clientIds;
  };

  clientAsyncInterface.forEach((method) => {
    this[method] = function () {
      let args = arguments;
      let key = args[0];
      let mapOutput = self.detailedMap(key, method);
      let activeClients = mapOutput.targets;

      if (mapOutput.type === 'single') {
        return activeClients[0][method].apply(activeClients[0], args);
      }

      let resultsPromises = activeClients.map((activeClient) => {
        return activeClient[method].apply(activeClient, args);
      });

      return Promise.all(resultsPromises);
    }
  });

  let multiKeyClientInterface = [
    'expire',
    'unexpire'
  ];

  multiKeyClientInterface.forEach((method) => {
    this[method] = function () {
      let args = arguments;
      let keys = args[0];
      let clientArgsMap = {};

      keys.forEach((key) => {
        let activeClients = self.map(key, method);
        activeClients.forEach((client) => {
          let clientId = client.id;
          if (clientArgsMap[clientId] == null) {
            clientArgsMap[clientId] = [];
          }
          clientArgsMap[clientId].push(key);
        });
      });

      let partArgs = Array.prototype.slice.call(args, 1);

      let resultsPromises = Object.keys(clientArgsMap).map((clientId) => {
        let activeClient = clients[clientId];
        let firstArg = clientArgsMap[clientId];
        let newArgs = [firstArg].concat(partArgs);
        return activeClient[method].apply(activeClient, newArgs);
      });

      return Promise.all(resultsPromises);
    };
  });

  clientUtils.forEach((method) => {
    this[method] = client[method].bind(client);
  });

  this.setMapper = function (mapperFunction) {
    mapper = mapperFunction;
  };

  this.getMapper = function (mapperFunction) {
    return mapper;
  };

  this.detailedMap = function (key, method) {
    let result = mapper(key, method, clientIds);
    let targets, type;
    if (typeof result === 'number') {
      type = 'single';
      targets = [clients[result % clients.length]];
    } else {
      type = 'multi';
      if (Array.isArray(result)) {
        let dataClients = [];
        result.forEach((res) => {
          dataClients.push(clients[res % clients.length]);
        });
        targets = dataClients;
      } else {
        targets = [];
      }
    }

    return {type: type, targets: targets};
  };

  this.map = function (key, method) {
    return self.detailedMap(key, method).targets;
  };

  this.destroy = function () {
    clients.forEach((client) => {
      client.closeAllListeners();
      client.end();
    });
  };
}

ClientCluster.prototype = Object.create(AsyncStreamEmitter.prototype);

module.exports.ClientCluster = ClientCluster;
