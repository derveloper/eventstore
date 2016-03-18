/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

/** @module eventstore-services-js/push_api */
var utils = require('vertx-js/util/utils');
var Vertx = require('vertx-js/vertx');

var io = Packages.io;
var JsonObject = io.vertx.core.json.JsonObject;
var JPushApi = eventstore.boundary.PushApi;

/**
 @class
*/
var PushApi = function(j_val) {

  var j_pushApi = j_val;
  var that = this;

  /**

   @public
   @param clientId {string} 
   @param address {string} 
   */
  this.subscribe = function(clientId, address) {
    var __args = arguments;
    if (__args.length === 2 && typeof __args[0] === 'string' && typeof __args[1] === 'string') {
      j_pushApi["subscribe(java.lang.String,java.lang.String)"](clientId, address);
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public
   @param clientId {string} 
   */
  this.unsubscribe = function(clientId) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'string') {
      j_pushApi["unsubscribe(java.lang.String)"](clientId);
    } else throw new TypeError('function invoked with invalid arguments');
  };

  // A reference to the underlying Java delegate
  // NOTE! This is an internal API and must not be used in user code.
  // If you rely on this property your code is likely to break if we change it / remove it without warning.
  this._jdel = j_pushApi;
};

/**

 @memberof module:eventstore-services-js/push_api
 @param vertx {Vertx} 
 @param address {string} 
 @return {PushApi}
 */
PushApi.createProxy = function(vertx, address) {
  var __args = arguments;
  if (__args.length === 2 && typeof __args[0] === 'object' && __args[0]._jdel && typeof __args[1] === 'string') {
    return utils.convReturnVertxGen(JPushApi["createProxy(io.vertx.core.Vertx,java.lang.String)"](vertx._jdel, address), PushApi);
  } else throw new TypeError('function invoked with invalid arguments');
};

// We export the Constructor function
module.exports = PushApi;