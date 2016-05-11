'use strict';

/**
 * Created by libinqi on 2016/5/10.
 */

var _ = require('lodash');

var Transation = function (connection) {
    this._connection = _.clone(connection);
    this._transaction = this._connection.transaction();
    connection = _.clone(connection, this);
    return connection;
};

Transation.prototype.start = function (callback) {
    var self = this;
    if (!self._connection.connected) {
        self._connection.connect(function (err, conn) {
            if (err) {
                callback(err);
                return;
            }
            self._connection = conn;
            self._transaction = conn.transaction();
            self._transaction.begin(callback);
        });
    }
    else {
        self._transaction.begin(callback);
    }
};

Transation.prototype.commit = function (callback) {
    var self = this;
    if (!self._connection.connected) {
        self._connection.connect(function (err, conn) {
            if (err) {
                callback(err);
                return;
            }
            self._connection = conn;
            self._transaction = conn.transaction();
            self._transaction.commit(callback);
        });
    }
    else {
        self._transaction.commit(callback);
    }
};

Transation.prototype.rollback = function (callback) {
    var self = this;
    if (!self._connection.connected) {
        self._connection.connect(function (err, conn) {
            if (err) {
                callback(err);
                return;
            }
            self._connection = conn;
            self._transaction = conn.transaction();
            self._transaction.rollback(callback);
        });
    }
    else {
        self._transaction.rollback(callback);
    }
};