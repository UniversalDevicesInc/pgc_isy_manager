'use strict'

const PACKAGE = require('./package.json')
const VERSION = PACKAGE.version

const rp = require('request-promise-native')
rp.debug = false
const xmlparser = require('xml2json-light')
const querystring = require('querystring')

// Parameters
const STAGE = process.env.STAGE
const LOCAL = process.env.LOCAL || false

const USER_TABLE = `pg_${STAGE}-usersTable`
const ISY_TABLE = `pg_${STAGE}-isysTable`
const NS_TABLE = `pg_${STAGE}-nsTable`

const SECRETS = require('./secrets')
const AWS_ACCESS_KEY_ID = SECRETS.get('SWARM_AWS_ACCESS_KEY_ID')
const AWS_SECRET_ACCESS_KEY = SECRETS.get('SWARM_AWS_SECRET_ACCESS_KEY')
if (AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY) {
  process.env['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
  process.env['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY
}

const MAX_RETRIES = 2

const AWS = require('aws-sdk')
AWS.config.update({region:'us-east-1', correctClockSkew: true})
const SQS = new AWS.SQS()

let PARAMS = {}
let USERCACHE = {}
let IOT

let keepAliveOptions = {
  keepAlive: true,
  //keepAliveMsecs: 300,
  //maxSockets: 10,
  //maxFreeSockets: 2
}

const httpsAgent = new require('https').Agent(keepAliveOptions)

async function configAWS() {
  IOT = new AWS.IotData({endpoint: `${PARAMS.IOT_ENDPOINT_HOST}`})
}

console.log(`Worker Manager Version: ${VERSION} :: Stage: ${STAGE}`)
console.log(`ENV: ${JSON.stringify(process.env)}`)

// Logger intercept for easy logging in the future.
const LOGGER = {
  info: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.log(`ISY: [${userId}] ${msg}`)
      let notificationTopic = `${STAGE}/frontend/${userId}`
      mqttSend(notificationTopic, {notification: {type: 'success', msg: msg}})
    } else {
      console.log(`ISY: ${msg}`)
    }
  },
  error: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.error(`ISY: [${userId}] ${msg}`)
      let notificationTopic = `${STAGE}/frontend/${userId}`
      mqttSend(notificationTopic, {notification: {type: 'error', msg: msg}})
    } else {
      console.error(`ISY: ${msg}`)
    }
  },
  debug: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.log(`ISY: [${userId}] ${msg}`)
    } else {
      console.log(`ISY: ${msg}`)
    }
  }
}

// DB Vars and Methods
const docOptions = {
  region: 'us-east-1',
}
var docClient = new AWS.DynamoDB.DocumentClient()

// DB Functions
async function getDbUser(id) {
  let params = {
    TableName: USER_TABLE,
    Key: {
      "id": id
    }
  }
  try {
    let data = await docClient.get(params).promise()
    if (data.hasOwnProperty('Item')) {
      let user = data.Item
      USERCACHE[id] = Object.assign(user, { timestamp: +Date.now() })
      return user
    }
    else {
      LOGGER.debug(`getDbUser: no record found`, id)
    }
  } catch (err) {
    LOGGER.error(`getDbUser: ${JSON.stringify(err, null, 2)}`, id)
  }
}

async function updateDbUser(userId, tokenData = false, isyKeys = false) {
  let user = {}
  let params = {
    TableName: USER_TABLE,
    Key: {
      "id": userId
    },
    ReturnValues: 'ALL_NEW'
  }
  if (tokenData) {
    params.UpdateExpression = `set tokenData = :t`,
    params.ExpressionAttributeValues= { ":t": tokenData }
  } else if (isyKeys) {
    params.UpdateExpression = `set isyKeys = :keys`,
    params.ExpressionAttributeValues = { ":keys": isyKeys }
  }
  try {
    let data = await docClient.update(params).promise()
    if (data.hasOwnProperty('Attributes')) {
      user = data.Attributes
      LOGGER.debug(`updateDbUser: Updated user JSON: ${JSON.stringify(user)}`, userId)
    }
  } catch (err) {
    LOGGER.error(`updateDbUser: ${JSON.stringify(err, null, 2)}`, userId)
  }
  return user
}

async function removeNodeServers(nodeServers, found, fullMsg) {
  for (let ns of found) {
    delete nodeServers[ns]
  }
  let removed = []
  for (let ns in nodeServers) {
    if (nodeServers[ns].type !== 'unmanaged') { continue }
    removed.push(ns)
    let params = {
      TableName: NS_TABLE,
      Key: {
        "id": `${nodeServers[ns].id}`,
        "profileNum": `${nodeServers[ns].profileNum}`
      },
      ReturnValues: 'NONE'
    }
    try {
      await docClient.delete(params).promise()
      LOGGER.debug(`NodeServer no longer found on ISY: ${nodeServers[ns].name}(${nodeServers[ns].profileNum}) Removed.`, fullMsg.userId)
    } catch (err) {
      LOGGER.error(`removeNodeServers: ${JSON.stringify(err, null, 2)}`, fullMsg.userId)
    }
  }
  return removed
}

async function updateUnmanagedNodeServer(id, ns, userId) {
  let update = {}
  let params = {
    TableName: NS_TABLE,
    Key: {
      "id": String(id),
      "profileNum": String(ns.profile)
    },
    UpdateExpression: `
      set #name = :name,
      #type = :type`,
    ExpressionAttributeNames: {
      "#name": 'name',
      "#type": 'type'
    },
    ExpressionAttributeValues: {
      ":name": ns.name,
      ":type": `unmanaged`
    },
    ReturnValues: 'ALL_NEW'
  }
  try {
    let data = await docClient.update(params).promise()
    if (data.hasOwnProperty('Attributes')) {
      update = data.Attributes
      LOGGER.debug(`updateUnmanagedNodeServer: Added/Updated NodeServer (${ns.profile})${ns.name} JSON: ${JSON.stringify(update)}`, userId)
    }
  } catch (err) {
    LOGGER.error(`updateUnmanagedNodeServer: ${id || 'none'} ${JSON.stringify(ns)}`, userId)
    LOGGER.error(`updateUnmanagedNodeServer: ${JSON.stringify(err, null, 2)}`, userId)
  }
  return update
}

async function getDbNodeServers(id, userId) {
  let returnValue = {}
  let params = {
    TableName: NS_TABLE,
    KeyConditionExpression: "id = :id",
    ExpressionAttributeValues: {
      ":id": id
    }
  }
  try {
    let data = await docClient.query(params).promise()
    if (data.Count > 0) {
      LOGGER.debug(`getDbNodeServers: Loaded ${data.Count} nodeserver(s) from the database.`, userId)
      for (let ns of data.Items) {
        returnValue[ns.profileNum] = ns
      }
    }
  } catch (err) {
    LOGGER.error(`getDbNodeServers: ${JSON.stringify(err, null, 2)}`, userId)
  }
  return returnValue
}

// Portal Functions
async function isyGetIsys(cmd, fullMsg) {
  // getIsys {}
  let api = 'api/isys'
  let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, true)
  if (!isyResponse.success) return LOGGER.error(`isyGetIsys: Connection to ISY unsuccessful. Please try your request again.`, fullMsg.userId)
  let isys = isyResponse.json
  let returnValue = {}

  if (!isys || !isys.hasOwnProperty('domain') || !isys.hasOwnProperty('data')) {
    LOGGER.error(`ISYS Response didn't have domain key.`, fullMsg.userId)
    return returnValue
  }
  let isyKeys = {}
  for (let isy of isys.data) {
    let alias
    for (let domain of isy.domain) {
      if (domain.id === isys.domain._id) {
        if (domain.hasOwnProperty('alias')) {
          alias = domain.alias
        }
      }
    }
    isyKeys[isy.uuid] = {
      key: isy.isyKey,
      online: isy.online
    }
    let params = {
      TableName: ISY_TABLE,
      Key: {
        "id": isy.uuid
      },
      UpdateExpression: `
        set alias = :alias,
        isyOnline = :isyOnline,
        isyData = :isyData,
        isyKey = :isyKey,
        userId = :userId`,
      ExpressionAttributeValues: {
        ":alias": alias || isy.uuid,
        ":isyOnline": isy.online,
        ":isyData": isy.isy,
        ":isyKey": isy.isyKey,
        ":userId": fullMsg.userId
      },
      ReturnValues: 'ALL_NEW'
    }
    try {
      let data = await docClient.update(params).promise()
      LOGGER.debug(`processIsys: Added/Updated ISY ${isy.uuid} JSON: ${JSON.stringify(data.Attributes)}`, fullMsg.userId)
      if (data.hasOwnProperty('Attributes')) {
        let cleanData = data.Attributes
        delete cleanData.isyKey
        returnValue[isy.uuid] = cleanData
      }
    } catch (err) {
      LOGGER.error(`processIsys: ${JSON.stringify(err, null, 2)}`, fullMsg.userId)
    }
  }
  makeResponse(cmd, fullMsg, returnValue, isyResponse)
  updateDbUser(fullMsg.userId, false, isyKeys)
}

// ISY Functions
async function isyGetNodeServers(cmd, fullMsg) {
  // getNodeServers {}
  let api = 'rest/profiles/ns/0/connection'
  let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, true)
  if (!isyResponse.success) return LOGGER.error(`isyGetNodeServers: Connection to ISY unsuccessful. Please try your request again.`, fullMsg.userId)
  let returnValue = await getDbNodeServers(fullMsg.id, fullMsg.userId)
  let nodeServers = isyResponse.json
  if (!nodeServers || !nodeServers.hasOwnProperty('connections')) {
    LOGGER.error(`isyGetNodeServers: Response wasn't XML or didn't have the connections property. This typically happens on ISY with firmware < 5.0`, fullMsg.userId)
  } else {
    let foundNS = []
    if (nodeServers.connections.hasOwnProperty('connection')) {
      if (Array.isArray(nodeServers.connections.connection)) {
        for (let ns of nodeServers.connections.connection) {
          foundNS.push(ns.profile)
          if (!returnValue.hasOwnProperty(ns.profile) || returnValue[ns.profile].type === 'unmanaged') {
            let updated = await updateUnmanagedNodeServer(fullMsg.id, ns, fullMsg.userId)
            if (updated) {
              returnValue[ns.profile] = updated
            }
          }
        }
      } else {
        let ns = nodeServers.connections.connection
        foundNS.push(ns.profile)
        if (!returnValue.hasOwnProperty(ns.profile) || returnValue[ns.profile].nstype === 'unmanaged') {
          let updated = await updateUnmanagedNodeServer(fullMsg.id, ns, fullMsg.userId)
          if (updated) {
            returnValue[ns.profile] = updated
          }
        }
      }
    }
    let removed = await removeNodeServers(JSON.parse(JSON.stringify(returnValue)), foundNS, fullMsg)
    for (let i of removed) {
      delete returnValue[i]
    }
  }
  makeResponse(cmd, fullMsg, returnValue, isyResponse)
}

async function isyAddNodeServer(cmd, fullMsg) {
  // addNodeServer { name: 'my name', isyUsername: 'pgc-cloud', isyPassword: 'blahblah', profileNum: '3' }
  try {
    let args = {
      ip: PARAMS.NS_ENDPOINT_HOST,
      baseurl: `${PARAMS.NS_ENDPOINT_BASEURL}${fullMsg.id}/${fullMsg.addNodeServer.profileNum}`,
      name: fullMsg.addNodeServer.name,
      nsuser: fullMsg.addNodeServer.isyUsername,
      nspwd: fullMsg.addNodeServer.isyPassword,
      isyusernum: 0,
      port: PARAMS.NS_ENDPOINT_PORT,
      timeout: 0,
      ssl: PARAMS.NS_ENDPOINT_SSL,
      enabled: true
    }
    let api = `rest/profiles/ns/${fullMsg.addNodeServer.profileNum}/connection/set/network`
    api += `?${querystring.stringify(args)}`
    let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, false)
    await makeResponse(cmd, fullMsg, fullMsg[cmd], isyResponse)
  } catch (err) {
    LOGGER.error(`isyAddNodeServer: ${err}`, fullMsg.userId)
  }
}

async function isyRemoveNodeServer(cmd, fullMsg) {
  // removeNodeServer { profileNum: '3' }
  let api = `rest/profiles/ns/${fullMsg.removeNodeServer.profileNum}/connection/remove`
  let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, false)
  await makeResponse(cmd, fullMsg, fullMsg[cmd], isyResponse)
}

async function isyAddNode(cmd, fullMsg, batch = false) {
  // addnode { address: 'abc123', primary: 'controller', name: 'ABC123 Node'
  //           nodedefid: 'nodeDef' <opt>nls: 'nlsstuff' }
  try {
    let address = addNodePrefix(fullMsg.profileNum, fullMsg[cmd].address)
    let primary = addNodePrefix(fullMsg.profileNum, fullMsg[cmd].primary)
    let args = {
      primary: primary,
      name: fullMsg[cmd].name
    }
    if (fullMsg[cmd].hasOwnProperty('nls')) {
      args.nls = fullMsg[cmd].nls
    }
    let api = `rest/ns/${fullMsg.profileNum}/nodes/${address}/add/${fullMsg[cmd].nodedefid}`
    api += `?${querystring.stringify(args)}`
    let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, true)
    if (isyResponse.statusCode === 200) {
      isyGroupNode(fullMsg, address, primary)
      if (fullMsg[cmd].hint) {
        isyAddHint(cmd, fullMsg, address)
      }
    }
    if (batch) {
      return Object.assign(fullMsg[cmd], isyResponse)
    }
    makeResponse(cmd, fullMsg, fullMsg[cmd], isyResponse, isyResponse.statusCode !== 200 ? false : true)
  } catch (err) {
    LOGGER.error(`isyAddNode: ${err}`, fullMsg.userId)
  }
}

async function isyAddHint(cmd, fullMsg, address) {
  try {
    let api = `rest/ns/${fullMsg.profileNum}/nodes/${address}/set/hint/${fullMsg[cmd].hint}`
    let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, true)
    if (isyResponse.statusCode === 200) {
      LOGGER.debug(`Successfully set ${address} hint to ${fullMsg[cmd].hint}`, fullMsg.userId)
    } else {
      LOGGER.error(`Failed to set ${address} hint to ${fullMsg[cmd].hint}. ${isyResponse.statusCode}`, fullMsg.userId)
    }
  } catch (err) {
    LOGGER.error(`isyAddHint: ${err}`, fullMsg.userId)
  }
}

async function isyRemoveNode(cmd, fullMsg, batch = false) {
  // removenode { address: 'abc123' }
  try {
    let address = addNodePrefix(fullMsg.profileNum, fullMsg.removenode.address)
    let api = `rest/ns/${fullMsg.profileNum}/nodes/${address}/remove`
    let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, false)
    if (batch) { return Object.assign({[cmd]: fullMsg[cmd]}, isyResponse) }
    makeResponse(cmd, fullMsg, fullMsg[cmd], isyResponse)
  } catch (err) {
    LOGGER.error(`isyRemoveNode: ${err}`, fullMsg.userId)
  }
}

async function isyUpdateNode(cmd, fullMsg) {
  try {
    let address = addNodePrefix(fullMsg.profileNum, fullMsg[cmd].address)
    let args
    if (fullMsg[cmd].hasOwnProperty('nls')) {
      args.nls = fullMsg[cmd].nls
    }
    let api = `rest/ns/${fullMsg.profileNum}/nodes/${address}/change/${fullMsg[cmd].nodedefid}`
    if (args) { api += `?${querystring.stringify(args)}` }
    let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, true)
    // makeResponse(cmd, fullMsg, fullMsg[cmd], isyResponse)
  } catch (err) {
    LOGGER.error(`isyUpdateNode: ${err}`, fullMsg.userId)
  }
}

async function isyStatus(cmd, fullMsg, batch = false) {
  // status { address: 'abc123', driver: 'ST', value: '12.3' uom: '5' }
  try {
    let address = addNodePrefix(fullMsg.profileNum, fullMsg.status.address)
    let api = `rest/ns/${fullMsg.profileNum}/nodes/${address}/report/status/${fullMsg.status.driver}`
    api += `/${fullMsg.status.value}/${fullMsg.status.uom}`
    let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, false)
    if (batch) {
      return Object.assign(fullMsg[cmd], isyResponse)
    }
    makeResponse(cmd, fullMsg, fullMsg[cmd], isyResponse)
  } catch (err) {
    LOGGER.error(`isyStatus: ${err}`, fullMsg.userId)
  }
}

async function isyCommand(cmd, fullMsg) {
  // command { address: 'abc123', command: 'DON', <opt>value: 'value' <opt>uom: '12' }
  try {
    let address = addNodePrefix(fullMsg.profileNum, fullMsg.command.address)
    let api = `rest/ns/${fullMsg.profileNum}/nodes/${address}/report/cmd/${fullMsg.command.command}`
    if (fullMsg.command.hasOwnProperty('value')) {
      api += `/${fullMsg.command.value}`
      if (fullMsg.command.hasOwnProperty('uom')) {
        api += `/${fullMsg.command.uom}`
      }
    }
    let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, false)
    // makeResponse(cmd, fullMsg, fullMsg[cmd], isyResponse)
  } catch (err) {
    LOGGER.error(`isyCommand: ${err}`, fullMsg.userId)
  }
}

async function isyReport(cmd, fullMsg) {
  // report { requestId: '123', success: true/false }
  try {
    let api = `rest/ns/${fullMsg.profileNum}/report/request/${fullMsg.report.requestId}`
    api += `/${fullMsg.report.success ? 'success' : 'fail'}`
    let isyResponse = await makeIsyRequest(api, fullMsg.userId, fullMsg.id, false)
    //await makeResponse(cmd, fullMsg, fullMsg[cmd], isyResponse)
  } catch (err) {
    LOGGER.error(`isyReport: ${err}`, fullMsg.userId)
  }
}

async function isyUploadProfile(cmd, fullMsg) {
  try {
    // await timeout((Math.floor(Math.random() * 10) + 1) * 1000)
    let api = `rest/ns/profile/${fullMsg.profileNum}/upload/${fullMsg[cmd].type}/${fullMsg[cmd].filename}`
    const fileData = Buffer.from(fullMsg[cmd].payload, 'base64').toString('utf8')
    let user = await getDbUser(fullMsg.userId)
    if (!user) { return LOGGER.error(`User not found with ID ${fullMsg.userId}`, fullMsg.userId) }
    let isy = false
    if (user.hasOwnProperty('isyKeys') && user.isyKeys.hasOwnProperty(fullMsg.id)) {
      isy = user.isyKeys[fullMsg.id]
    }
    if (!isy) { return LOGGER.error(`ISY not found with ID: ${fullMsg.id}`, fullMsg.userId) }
    api = `${isy.key}/${api}`
    if (!PARAMS.UDI_BASE) { return }
    let url = `${PARAMS.UDI_BASE}/${api}`
    let options = {
      agent: httpsAgent,
      method: 'POST',
      headers: {
        'content-type': 'application/xml',
        'Authorization': `Bearer ${PARAMS.PORTAL_SERVICE_TOKEN}`,
        'From': `${fullMsg.userId}`
      },
      url: url,
      body: fileData,
      resolveWithFullResponse: true,
      simple: false,
      timeout: 5000,
      gzip: true
    }
    const res = await rp(options)
    if (res && res.statusCode === 200) {
      LOGGER.info(`Sucessfully uploaded ${fullMsg[cmd].filename} to ISY.`, fullMsg.userId)
    } else {
      LOGGER.error(`Failed to upload ${fullMsg[cmd].filename} to ISY. Status Code: ${res.statusCode}. Try again using the portal "Profile Upload" button.`, fullMsg.userId)
    }
  } catch (err) {
    LOGGER.error(`isyUploadProfile: ${err.stack}`, fullMsg.userId)
  }
}

async function isyGroupNode(fullMsg, address, primary) {
  try {
    let api = `services`
  	if (address === primary) return
    let data = `<s:Envelope>
                  <s:Body>
                    <u:SetParent xmlns:u="urn:udi-com:service:X_Insteon_Lighting_Service:1">
                      <node>${address}</node>
                      <nodeType>1</nodeType>
                      <parent>${primary}</parent>
                      <parentType>1</parentType>
                    </u:SetParent>
                  </s:Body>
                </s:Envelope>`
    let user = await getDbUser(fullMsg.userId)
    if (!user) { return LOGGER.error(`User not found with ID ${fullMsg.userId}`, fullMsg.userId) }
    let isy = false
    if (user.hasOwnProperty('isyKeys') && user.isyKeys.hasOwnProperty(fullMsg.id)) {
      isy = user.isyKeys[fullMsg.id]
    }
    if (!isy) { return LOGGER.error(`ISY not found with ID: ${fullMsg.id}`, fullMsg.userId) }
    api = `${isy.key}/${api}`
    if (!PARAMS.UDI_BASE) { return }
    let url = `${PARAMS.UDI_BASE}/${api}`
    let options = {
      agent: httpsAgent,
      method: 'POST',
      headers: {
        'content-type': 'application/x-www-form-urlencoded',
        'SOAPACTION': 'urn:udi-com:service:X_Insteon_Lighting_Service:1#SetParent',
        'Authorization': `Bearer ${PARAMS.PORTAL_SERVICE_TOKEN}`,
        'From': `${fullMsg.userId}`
      },
      url: url,
      body: data,
      resolveWithFullResponse: true,
      simple: false,
      timeout: 5000,
      gzip: true
    }
    const res = await rp(options)
    if (res && res.statusCode === 200) {
      LOGGER.debug(`Successfully grouped ${address} under ${primary}`, fullMsg.userId)
    } else {
      LOGGER.error(`Failed to group ${address} under ${primary}. Status Code: ${res.statusCode}.`, fullMsg.userId)
    }
  } catch (err) {
    LOGGER.error(`isyGroupNode: ${err.stack}`, fullMsg.userId)
  }
}

async function batch(cmd, fullMsg) {
  let wrapData = JSON.parse(JSON.stringify(fullMsg)) // deepcopy hack
  delete wrapData[cmd] // leave just userId, topic, profileNum, id
  let tasks = []
  let results = {batch: {}}
  for (let key in fullMsg[cmd]) {
    if (!['addnode', 'removenode', 'status'].includes(key)) { continue }
    let command = checkCommand(key)
    let type = fullMsg[cmd][key]
    if (Array.isArray(type) && type.length) {
      for (let item of type) {
        if (verifyProps(item, apiSwitch[key].props).valid) {
          tasks.push(command.func(key, {
            userId: fullMsg.userId,
            id: fullMsg.id,
            profileNum: fullMsg.profileNum,
            topic: fullMsg,
            [key]: item
          }, true))
        }
      }
      results.batch[key] = []
      for (let task of tasks) {
        results.batch[key].push(await task)
      }
    }
  }
  LOGGER.debug(`batch response: ${JSON.stringify(results)}`)
  makeResponse(cmd, fullMsg, fullMsg[cmd], results)
}

// Send to ISY
async function makeIsyRequest(api, userId, id = false, fullResponse = false) {
  let response = {
    json: false,
    statusCode: null,
    elapsed: 0,
    success: false,
    error: false,
    seq: false
  }
  try {
    var hrstart = process.hrtime()
    let user = USERCACHE[userId] || await getDbUser(userId)
    if (!user) {
        LOGGER.error(`User not found with ID: ${userId}`, userId)
        response.error = `User not found with ID: ${userId}`
        return response
    }
    if (!PARAMS.UDI_BASE || !PARAMS.PORTAL_SERVICE_TOKEN) {
        response.error = `ISYreq: UDI_BASE or PORTAL_SERVICE_TOKEN not found.`
        return response
    }
    if (id) {
      let isy = false
      if (user.hasOwnProperty('isyKeys') && user.isyKeys.hasOwnProperty(id)) {
        isy = user.isyKeys[id]
      }
      if (!isy) {
        LOGGER.error(`ID Specified but ISY not found with ID: ${id}`, userId)
        response.error = `ID Specified but ISY not found with ID: ${id}`
        return response
      }
      api = `${isy.key}/${api}`
    }
    let url = `${PARAMS.UDI_BASE}/${api}`
    const apiOptions = {
      agent: httpsAgent,
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${PARAMS.PORTAL_SERVICE_TOKEN}`,
        'From': `${userId}`
      },
      uri: url,
      resolveWithFullResponse: true,
      simple: false,
      timeout: 5000,
      gzip: true,
    }
    for (let i = 0; i <= MAX_RETRIES; i++) {
      try {
        let res = await rp(apiOptions)
        if (res && (res.statusCode === 500 || res.statusCode === 401)) {
          LOGGER.error(`ISYreq: url: ${url} :: statuscode ${res.statusCode} :: ${JSON.stringify(res.body)}`, userId)
          response.statusCode = res.statusCode
          response.error = `ISYreq: url: ${url} :: statuscode ${res.statusCode} :: ${JSON.stringify(res.body)}`
          return response
        }
        if (res && res.statusCode === 200) { response.success = true }
        // LOGGER.debug(`${JSON.stringify(response)}`, userId)
        if (fullResponse) {
          try {
            response.json = JSON.parse(res.body)
          } catch (error) {
            if (error instanceof SyntaxError) {
              response.json = xmlparser.xml2json(res.body)
            } else {
              LOGGER.error(`ISYreq: ${error.stack}`, userId)
              response.error = `${error.stack}`
            }
          }
        }
        response.statusCode = res.statusCode
        response.elapsed = process.hrtime(hrstart)[1]/1000000 + 'ms'
        LOGGER.debug(`${response.statusCode} :: ${response.elapsed} - ${url}`, userId)
        break
      } catch (err) {
        if (i >= MAX_RETRIES) {
          LOGGER.error(`connection exceeded max re-tries. Aborting. ${err.message}`, userId)
          response.error = err.message
          return response
        } else {
          LOGGER.error(`connection failed. Retrying... ${err.message}`, userId)
        }
      }
    }
  } catch (err) {
    LOGGER.error(`ISYreqouter: ${err.stack}`, userId)
    response.error = `${err.stack}`
  }
  return response
}

// MQTT Send
async function mqttSend(topic, message, qos = 0) {
  const payload = JSON.stringify(message)
  const iotMessage = {
    topic: topic,
    payload: payload,
    qos: qos
  }
  return IOT.publish(iotMessage).promise()
}

// Make MQTT Response
async function makeResponse(cmd, fullMsg, returnObject, isyResponse = {}, cleanJson = true) {
  let response = {}
  for (let prop of ['userId', 'id', 'topic', 'profileNum']) {
    if (fullMsg.hasOwnProperty(prop)) {
      response[prop] = fullMsg[prop]
    }
  }
  if (cleanJson) {
    isyResponse.json = {}
  }
  response['result'] = isyResponse
  response['result'][cmd] = fullMsg[cmd]
  response[cmd] = returnObject
  if (fullMsg.hasOwnProperty('seq')) { response['seq'] = fullMsg.seq }
  mqttSend(fullMsg.topic, response)
}

// Intercept response body and do backend process
const checkCommand = (type) => apiSwitch[type] || null

const apiSwitch = {
  getIsys: {
    props: [],
    func: isyGetIsys,
  },
  getNodeServers: {
    props: [],
    func: isyGetNodeServers
  },
  addNodeServer: {
    props: ['name', 'isyUsername', 'isyPassword', 'profileNum'],
    func: isyAddNodeServer
  },
  removeNodeServer: {
    props: ['profileNum'],
    func: isyRemoveNodeServer
  },
  addnode: {
    props: ['address', 'primary', 'name', 'nodedefid'],
    func: isyAddNode
  },
  removenode: {
    props: ['address'],
    func: isyRemoveNode
  },
  updatenode: {
    props: ['address', 'primary', 'name', 'nodedefid'],
    func: isyUpdateNode
  },
  uploadProfile: {
    props: ['type', 'filename', 'payload'],
    func: isyUploadProfile
  },
  status: {
    props: ['address', 'driver', 'value', 'uom'],
    func: isyStatus
  },
  command: {
    props: ['address', 'command'],
    func: isyCommand
  },
  batch: {
    props: [],
    func: batch
  },
  report: {
    props: ['requestId', 'success'],
    func: isyReport
  },
}

// Helper methods
const propExists = (obj, path) => {
  return !!path.split(".").reduce((obj, prop) => {
      return obj && obj[prop] ? obj[prop] : undefined;
  }, obj)
}

const verifyProps = (message, props) => {
  let confirm = {
    valid: true,
    missing: null
  }
  for (let prop of props) {
    if (!propExists(message, prop)) {
      confirm.valid = false
      confirm.missing = prop
      break
    }
  }
  return confirm
}

function addNodePrefix(profileNum, nid) {
  return `n${('00' + profileNum).slice(-3)}_${nid}`.slice(0, 20)
}

function addressRight(address) {
  return address.substring(address.indexOf('_')+1)
}

const timeout = ms => new Promise(run => setTimeout(run, ms))

async function processMessage(message) {
  //UDI_BASE = process.env.UDI_BASE
  //; ({notificationTopic, userId} = [null, null])
  let props = verifyProps(message, ['userId', 'topic'])
  if (!props.valid) {
    return LOGGER.error(`Request missing required property: ${props.missing} :: ${JSON.stringify(message)}`)
  }
  //; ({userId} = message)
  //notificationTopic = `${process.env.STAGE}/frontend/${userId}`
  LOGGER.debug(JSON.stringify(message), message.userId)
  for (let key in message) {
    if (['userId', 'id', 'topic', 'profileNum'].includes(key)) { continue }
    try {
      let command = checkCommand(key)
      if (!command) { continue }
      let props = verifyProps(message[key], apiSwitch[key].props)
      if (!props.valid) {
        return LOGGER.error(`${key} was missing ${props.missing} :: ${JSON.stringify(message)}`, message.userId)
      }
      command.func(key, message)
    } catch (error) {
      LOGGER.error(`${error}`, message.userId)
    }
  }
}

async function getMessages() {
  const params = {
    MaxNumberOfMessages: 10,
    QueueUrl: PARAMS.SQS_ISY,
    WaitTimeSeconds: 10
  }
  let deletedParams = {
    Entries: [],
    QueueUrl: PARAMS.SQS_ISY
  }
  try {
    LOGGER.info(`Getting messages...`)
    let data = await SQS.receiveMessage(params).promise()
    if (data.Messages) {
        LOGGER.info(`Got ${data.Messages.length} message(s)`)
        let tasks = []
        for (let message of data.Messages) {
          try {
            let body = JSON.parse(message.Body)
            let msg = body.msg
            LOGGER.info(`Got Message: ${JSON.stringify(msg)}`)
            tasks.push(processMessage(msg))
          } catch (err) {
            LOGGER.error(`Message not JSON: ${message.Body}`)
          }
          deletedParams.Entries.push({
              Id: message.MessageId,
              ReceiptHandle: message.ReceiptHandle
          })
        }
        for (let task of tasks) {
          await task
        }
        let deleted = await SQS.deleteMessageBatch(deletedParams).promise()
        deletedParams.Entries = []
        LOGGER.info(`Deleted Messages: ${JSON.stringify(deleted)}`)
    } else {
      LOGGER.info(`No messages`)
    }
  } catch (err) {
    LOGGER.error(err.stack)
  }
}

async function getParameters(nextToken) {
  const ssm = new AWS.SSM()
  var ssmParams = {
    Path: `/pgc/${STAGE}/`,
    MaxResults: 10,
    Recursive: true,
    NextToken: nextToken,
    WithDecryption: true
  }
  let params = await ssm.getParametersByPath(ssmParams).promise()
  if (params.Parameters.length === 0) throw new Error(`Parameters not retrieved. Exiting.`)
  for (let param of params.Parameters) {
    PARAMS[param.Name.split('/').slice(-1)[0]] = param.Value
  }
  if (params.hasOwnProperty('NextToken')) {
    await getParameters(params.NextToken)
  }
}

async function startHealthCheck() {
  require('http').createServer(function(request, response) {
    if (request.url === '/health' && request.method ==='GET') {
        //AWS ELB pings this URL to make sure the instance is running smoothly
        let data = JSON.stringify({uptime: process.uptime()})
        response.writeHead(200, {'Content-Type': 'application/json'})
        response.write(data)
        response.end()
    }
  }).listen(3000)
}

async function checkUserCache() {
  let validDuration = 1000 * 60 * 60 * 24 // 24 Hours
  let currentTime = +Date.now()
  for (let user in USERCACHE) {
    if (currentTime - USERCACHE[user].timestamp > validDuration) {
      delete USERCACHE[user]
    }
  }
}

async function main() {
  await getParameters()
  if (!PARAMS.PORTAL_SERVICE_TOKEN) {
    LOGGER.error(`No Service Token retrieved. Exiting.`)
    process.exit(1)
  }
  LOGGER.info(`Retrieved Parameters from AWS Parameter Store for Stage: ${STAGE}`)
  await configAWS()
  startHealthCheck()
  setInterval(checkUserCache, 15 * 60 * 1000) // run every 15 minutes
  try {
    while (true) {
      await getMessages()
    }
  } catch(err) {
    LOGGER.error(err.stack)
    main()
  }

}

['SIGINT', 'SIGTERM'].forEach(signal => {
  process.on(signal, () => {
    LOGGER.debug('Shutdown requested. Exiting...')
    setTimeout(() => {
      process.exit()
    }, 500)
  })
})

main()