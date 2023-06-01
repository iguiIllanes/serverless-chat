"use strict";

const AWS = require("aws-sdk");
const dynamo = new AWS.DynamoDB.DocumentClient();

const CONNECTION_DB_TABLE = process.env.CONNECTION_DB_TABLE;
const GROUP_TABLE = process.env.GROUP_TABLE;

const successfullResponse = {
  statusCode: 200,
  body: "Success",
};

const failedResponse = (statusCode, error) => ({
  statusCode,
  body: error,
});

/*
 * CONNECT
 * */
module.exports.connectHandler = (event, _context, callback) => {
  addConnection(event.requestContext.connectionId)
    .then(() => {
      callback(null, successfullResponse);
    })
    .catch((err) => {
      callback(failedResponse(500, JSON.stringify(err)));
    });
};

const addConnection = (connectionId) => {
  const params = {
    TableName: CONNECTION_DB_TABLE,
    Item: {
      connectionId: connectionId,
    },
  };

  return dynamo.put(params).promise();
};

/*
 * DISCONNECT
 * */
module.exports.disconnectHandler = (event, _context, callback) => {
  deleteConnection(event.requestContext.connectionId)
    .then(() => {
      callback(null, successfullResponse);
    })
    .catch((err) => {
      console.log(err);
      callback(failedResponse(500, JSON.stringify(err)));
    });
};

const deleteConnection = (connectionId) => {
  const params = {
    TableName: CONNECTION_DB_TABLE,
    Key: {
      connectionId: connectionId,
    },
  };

  //TODO: delete connection from GROUP_TABLE

  return dynamo.delete(params).promise();
};

/*
 * DEFAULT
 * */
module.exports.defaultHandler = (_event, _context, callback) => {
  callback(null, failedResponse(404, "No event found"));
};

/*
 * GROUPS
 * */

module.exports.createGroupHandler = (event, _context, callback) => {
  console.log("CreateGroupHandler invocado");
  createGroup(event)
    .then(() => {
      callback(null, successfullResponse);
    })
    .catch((err) => {
      console.log("Error en createGroupHandler", err.message);
      callback(failedResponse(500, JSON.stringify(err)));
    });
};

const createGroup = (event) => {
  console.log("Llegando a createGroup");
  const body = JSON.parse(event.body);
  console.log("body", body);
  const params = {
    TableName: GROUP_TABLE,
    Item: {
      createdBy: event.requestContext.connectionId,
      groupName: body.groupName,
      members: [event.requestContext.connectionId],
    },
  };

  console.log("params", params);

  return dynamo.put(params).promise();
};

module.exports.joinGroupHandler = (event, _context, callback) => {
  joinGroup(event)
    .then(() => {
      callback(null, successfullResponse);
    })
    .catch((err) => {
      callback(failedResponse(500, JSON.stringify(err)));
    });
};

const joinGroup = (event) => {
  const body = JSON.parse(event.body);
  const params = {
    TableName: GROUP_TABLE,
    Key: {
      groupName: body.groupName,
    },
    UpdateExpression: "SET members = list_append(members, :connectionId)",
    ExpressionAttributeValues: {
      ":connectionId": [event.requestContext.connectionId],
    },
  };

  return dynamo.update(params).promise();
};

const getGroupMembers = (event) => {
  //TODO: check if this works
  const body = JSON.parse(event.body);
  const params = {
    TableName: GROUP_TABLE,
    Key: {
      groupName: body.groupName,
    },
  };

  return dynamo.get(params).promise();
};

/*
 * SEND MESSAGES
 * */

module.exports.sendMessageHandler = (event, _context, callback) => {
  sendMessageToId(event) //FIXME: change to sendMessageToConnectionId
    .then(() => {
      callback(null, successfullResponse);
    })
    .catch((err) => {
      callback(failedResponse(500, JSON.stringify(err)));
    });
};

const sendMessageToId = (event) => {
  const body = JSON.parse(event.body);
  const connectionId = body.connectionId;
  return send(event, connectionId);
};

module.exports.broadcastMessageHandler = (event, _context, callback) => {
  console.log("BroadcastingMessageHandler invocado");
  broadcastMessage(event)
    .then(() => {
      callback(null, successfullResponse);
    })
    .catch((err) => {
      callback(failedResponse(500, JSON.stringify(err)));
    });
};

const broadcastMessage = (event) => {
  console.log("Llegando a broadcastMessage");
  return getAllConnections().then((connectionData) => {
    console.log("Datos de conexion en broadcastMessage", connectionData);
    return connectionData.Items.map((connectionId) => {
      console.log("Enviando mensaje a ", connectionId);
      return send(event, connectionId.connectionId);
    });
  });
};

const getAllConnections = () => {
  const params = {
    TableName: CONNECTION_DB_TABLE,
    ProjectionExpression: "connectionId",
  };

  return dynamo.scan(params).promise();
};

module.exports.sendMessageToGroupHandler = (event, _context, callback) => {
  sendMessageToGroup(event)
    .then(() => {
      callback(null, successfullResponse);
    })
    .catch((err) => {
      callback(failedResponse(500, JSON.stringify(err)));
    });
};

const sendMessageToGroup = (event) => {
  const body = JSON.parse(event.body);
  const groupName = body.groupName;
  return getGroupMembers(event).then((connectionData) => {
    console.log(`Members in ${groupName}`, connectionData);
    return connectionData.Items.map((connectionId) => {
      return send(event, connectionId.connectionId);
    });
  });
};

const send = (event, connectionId) => {
  console.log("Llegando a send");
  const body = JSON.parse(event.body);
  let postData = body.data;
  console.log("Sending...", postData);

  const endpoint =
    event.requestContext.domainName + "/" + event.requestContext.stage;
  const apigwManagementApi = new AWS.ApiGatewayManagementApi({
    apiVersion: "2018-11-29",
    endpoint: endpoint,
  });
  console.log("hasta aqui");

  const params = {
    ConnectionId: connectionId,
    Data: postData,
  };
  console.log("Haciendo el postToConnection con params", params);
  return apigwManagementApi.postToConnection(params).promise();
};
