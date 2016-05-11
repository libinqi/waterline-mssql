'use strict';

/**
 * Created by libinqi on 2016/5/10.
 */

var _ = require('lodash');
var mssql = require('mssql');
var utils = require('./utils');

var Transation = function (connection) {
    this.config = _.clone(connection);
    this.connection = null;
    this._transaction = null;
    this.transactioned = false;
};

Transation.prototype.connect = function (callback) {
    var self = this;
    if (self.connection && self.connection.connected) {
        callback(null, self.connection);
    }
    else {
        self.connection = new mssql.Connection(utils.marshalConfig(this.config), function (err) {
            callback(null, self.connection);
        });
    }
}

Transation.prototype.disconnect = function () {
    if (this.connection && this.connection.connected) {
        this.connection.close();
    }
    this.connection = null;
}

// Transation.prototype.connection = function () {
//     return this.connection;
// }

Transation.prototype.start = function (callback) {
    var self = this;

    self.connect(function (err, _connection) {
        if (err)callback && callback(err);
        else {
            self._transaction = _connection.transaction();
            self._transaction.begin(function (err) {
                if (err)callback && callback(err);
                else {
                    self.transactioned = true;
                    callback && callback(null);
                }

            });
        }
    });
};

Transation.prototype.commit = function (callback) {
    var self = this;

    self.connect(function (err) {
        if (err)callback && callback(err);
        if (self._transaction) {
            self._transaction.commit(function (err) {
                if (err) {
                    self._transaction.rollback();
                    callback && callback('事务提交失败,因为事务过期或没有开始');
                }
                self.disconnect();
                self.transactioned = false;
            });
        }
        else {
            callback && callback('请调用transaction.start()开启事务');
        }
    });
};

Transation.prototype.rollback = function (callback) {
    var self = this;

    // self.disconnect();

    self.connect(function (err) {
        if (err)callback && callback(err);
        if (self._transaction) {
            self._transaction.rollback(function (err) {
                if (err) {
                    callback && callback('事务回滚失败,因为事务过期或没有开始');
                }
                self.disconnect();
                self.transactioned = false;
            });
        }
        else {
            callback && callback('请调用transaction.start()开启事务');
        }
    });
};

module.exports = Transation;