var async = require('async');
var EventEmitter = require('events').EventEmitter;

var ClientCluster = function (clients) {
  var self = this;

  var handleMessage = function () {
    var args = Array.prototype.slice.call(arguments);
    self.emit.apply(self, ['message'].concat(args));
  };

  clients.forEach((client) => {
    client.on('message', handleMessage);
  });

  var i, method;
  var client = clients[0];
  var clientIds = [];

  var clientAsyncInterface = [
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

  var clientUtils = [
    'extractKeys',
    'extractValues'
  ];

  clients.forEach((client, i) => {
    client.on('error', (error) => {
      this.emit('error', error);
    });
    client.on('warning', (warning) => {
      this.emit('warning', warning);
    });
    client.id = i;
    clientIds.push(i);
  });

  // Default mapper maps to all clients.
  var mapper = function () {
    return clientIds;
  };

  clientAsyncInterface.forEach((method) => {
    this[method] = function () {
      var args = arguments;
      var key = args[0];
      var mapOutput = self.detailedMap(key, method);
      var activeClients = mapOutput.targets;

      if (mapOutput.type === 'single') {
        return activeClients[0][method].apply(activeClients[0], args);
      }

      var resultsPromises = activeClients.map((activeClient) => {
        return activeClient[method].apply(activeClient, args);
      });

      return Promise.all(resultsPromises);
    }
  });

  var multiKeyClientInterface = [
    'expire',
    'unexpire'
  ];

  multiKeyClientInterface.forEach((method) => {
    this[method] = function () {
      var args = arguments;
      var keys = args[0];
      var clientArgsMap = {};

      keys.forEach((key) => {
        var activeClients = self.map(key, method);
        activeClients.forEach((client) => {
          var clientId = client.id;
          if (clientArgsMap[clientId] == null) {
            clientArgsMap[clientId] = [];
          }
          clientArgsMap[clientId].push(key);
        });
      });

      var partArgs = Array.prototype.slice.call(args, 1);

      var resultsPromises = Object.keys(clientArgsMap).map((clientId) => {
        var activeClient = clients[clientId];
        var firstArg = clientArgsMap[clientId];
        var newArgs = [firstArg].concat(partArgs);
        return activeClient[method].apply(activeClient, newArgs);
      });

      return Promise.all(resultsPromises);
    };
  });

  // TODO 2: Test
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
    var result = mapper(key, method, clientIds);
    var targets, type;
    if (typeof result === 'number') {
      type = 'single';
      targets = [clients[result % clients.length]];
    } else {
      type = 'multi';
      if (Array.isArray(result)) {
        var dataClients = [];
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
};

ClientCluster.prototype = Object.create(EventEmitter.prototype);

module.exports.ClientCluster = ClientCluster;
