const ArgumentType = require('../../extension-support/argument-type');
const BlockType = require('../../extension-support/block-type');
const Cast = require('../../util/cast');
const log = require('../../util/log');
const EventBus = require('vertx3-eventbus-client');

const ACTUAL_MESSAGE = 'ACTUAL_MESSAGE';

class Scratch3Vertx {

    constructor (runtime) {
        this.runtime = runtime;

        this.eventBus = null;

        this.runtime.on('PROJECT_STOP_ALL', () => this._stopEBConnection());

        this.messages = {};
    }

    getInfo () {
        return {
            id: 'vertx',
            name: 'Vert.x Event Bus',
            blocks: [
                {
                    opcode: 'connect',
                    blockType: BlockType.COMMAND,
                    text: 'connect [ADDRESS]',
                    arguments: {
                        ADDRESS: {
                            type: ArgumentType.STRING,
                            defaultValue: 'http://localhost:8080/eventbus'
                        }
                    }
                },
                {
                    opcode: 'send',
                    blockType: BlockType.COMMAND,
                    text: 'send [ADDRESS] [PAYLOAD]',
                    arguments: {
                        ADDRESS: {
                            type: ArgumentType.STRING,
                            defaultValue: 'myservice'
                        },
                        PAYLOAD: {
                            type: ArgumentType.STRING,
                            defaultValue: 'hello'
                        }
                    }
                },
                {
                    opcode: 'request',
                    blockType: BlockType.REPORTER,
                    text: 'request [ADDRESS] [PAYLOAD]',
                    arguments: {
                        ADDRESS: {
                            type: ArgumentType.STRING,
                            defaultValue: 'myservice'
                        },
                        PAYLOAD: {
                            type: ArgumentType.STRING,
                            defaultValue: ''
                        }
                    }
                },
                {
                    opcode: 'whenReceive',
                    blockType: BlockType.HAT,
                    text: 'when receive [ADDRESS]',
                    arguments: {
                        ADDRESS: {
                            type: ArgumentType.STRING,
                            defaultValue: 'myservice'
                        }
                    }
                },
                {
                    opcode: 'getPayload',
                    blockType: BlockType.REPORTER,
                    text: 'get payload'
                },
                {
                    opcode: 'reply',
                    blockType: BlockType.COMMAND,
                    text: 'reply [PAYLOAD]',
                    arguments: {
                        PAYLOAD: {
                            type: ArgumentType.STRING,
                            defaultValue: 'Hello!'
                        }
                    }
                }
            ],
            menus: {}
        };
    }

    _checkEBConnection () {
        return this.eventBus !== null && this.eventBus.state === EventBus.OPEN;
    }

    _stopEBConnection () {
        if (this.eventBus !== null) {
            this.eventBus.close();
            this.eventBus = null;
        }
    }

    _listenerExists (address) {
        return this.messages.hasOwnProperty(address);
    }

    _startListen (address) {
        if (this._checkEBConnection()) {
            this.messages[address] = [];
            this.eventBus.registerHandler(address, (err, message) => {
                if (err === null) {
                    log.log(`Received message from ${address}: ${message.body}`);
                    this.messages[address].push(message);
                } else {
                    log.log(`Received error from ${address}: ${err}`);
                }
            });
            log.log(`Registered handler for ${address}`);
        }
    }

    _restartAllListeners () {
        if (this._checkEBConnection()) {
            Object.keys(this.messages).forEach(addr => {
                this._startListen(addr);
            });
        }
    }

    _getMessageIfAny (address) {
        return this.messages[address].shift();
    }

    connect (args) {
        const address = Cast.toString(args.ADDRESS);
        if (this._checkEBConnection()) {
            log.log(`Connection already opened ${address}`);
            return Promise.resolve();
        }
        log.log(`Trying to connect to Vert.x event bus bridge at address ${address}`);
        this.eventBus = new EventBus(address);
        return new Promise((resolve, reject) => {
            this.eventBus.onopen = () => {
                log.log(`Connection opened to ${address}`);
                this._restartAllListeners();
                resolve();
            };
            this.eventBus.onerror = () => {
                log.log(`Error while handling connection to ${address}`);
                reject();
            };
            this.eventBus.onclose = () => {
                log.log(`Connection closed to ${address}`);
            };
        });
    }

    send (args) {
        const address = Cast.toString(args.ADDRESS);
        const payload = Cast.toString(args.PAYLOAD);
        if (this._checkEBConnection()) {
            log.log(`Trying to send message ${payload} to Vert.x event bus bridge at address ${address}`);
            this.eventBus.publish(address, payload);
        } else {
            log.log(`Cannot send message, we are not connected to the event bus ${address}`);
        }
    }

    request (args) {
        const address = Cast.toString(args.ADDRESS);
        const payload = Cast.toString(args.PAYLOAD);
        if (this._checkEBConnection()) {
            log.log(`Trying to send message ${payload} to Vert.x event bus bridge at address ${address}`);
            return new Promise((resolve, reject) => {
                this.eventBus.send(address, payload, {}, (error, message) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve(JSON.stringify(message.body));
                    }
                });
            });
        }
        log.log(`Cannot request message, we are not connected to the event bus ${address}`);
        return Promise.resolve();
    }

    whenReceive (args, util) {
        const address = Cast.toString(args.ADDRESS);
        if (!this._listenerExists(address)) {
            this._startListen(address, util);
            return false;
        }

        const message = this._getMessageIfAny(address);

        // eslint-disable-next-line no-undefined
        if (message === undefined || message === null) {
            return false;
        }

        util.thread.pushThreadParam(ACTUAL_MESSAGE, message);
        return true;
    }

    getPayload (args, util) {
        const message = util.thread.getThreadParam(ACTUAL_MESSAGE);
        if (message === null) {
            log.log('Cannot find any message in the context!');
            return Promise.reject('Cannot find any message in the context!');
        }

        return Promise.resolve(message.body);
    }

    reply (args, util) {
        const message = util.thread.getThreadParam(ACTUAL_MESSAGE);
        if (message === null) {
            log.log('Cannot find any message in the context!');
            return Promise.reject('Cannot find any message in the context!');
        }
        message.reply(Cast.toString(args.PAYLOAD));
        return Promise.resolve();
    }
}

module.exports = Scratch3Vertx;
