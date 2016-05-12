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
    this.transaction = null;
    this.transactioned = false;
    this.timeout = 2;
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

Transation.prototype.start = function (isolationLevel) {
    var self = this;

    if (isolationLevel) {
        switch (isolationLevel) {
            case 'READ_UNCOMMITTED':
                isolationLevel = mssql.ISOLATION_LEVEL.READ_UNCOMMITTED;
                break;
            case 'REPEATABLE_READ':
                isolationLevel = mssql.ISOLATION_LEVEL.REPEATABLE_READ;
                break;
            case 'SERIALIZABLE':
                isolationLevel = mssql.ISOLATION_LEVEL.SERIALIZABLE;
                break;
            case 'SNAPSHOT':
                isolationLevel = mssql.ISOLATION_LEVEL.SNAPSHOT;
                break;
            default:
                isolationLevel = mssql.ISOLATION_LEVEL.READ_COMMITTED;
                break;
        }
    }
    else {
        isolationLevel = mssql.ISOLATION_LEVEL.READ_COMMITTED;
    }

    return new Promise(function (resolve, reject) {
        self.connect(function (err, _connection) {
            if (err)reject(err);
            else {
                self.transaction = _connection.transaction();

                self.transaction.begin(isolationLevel, function (err) {
                    if (err)reject(err);
                    else {
                        self.transactioned = true;
                        resolve();

                        setTimeout(function () {
                            if (self.transactioned) {
                                self.rollback().catch(function (err) {
                                    reject(err);
                                });
                            }
                        }, self.timeout * 1000);
                    }
                });
            }
        });
    });
};

Transation.prototype.commit = function () {
    var self = this;

    return new Promise(function (resolve, reject) {
        self.connect(function (err) {
            if (err)reject(err);
            if (self.transaction) {
                self.transaction.commit(function (err) {
                    if (err) {
                        self.transaction.rollback();
                        reject('事务提交失败,因为事务过期或没有开始');
                    }
                    self.disconnect();
                    self.transactioned = false;
                    resolve();
                });
            }
            else {
                reject('请调用transaction.start()开启事务');
            }
        });
    });
};

Transation.prototype.rollback = function () {
    var self = this;

    // self.disconnect();
    return new Promise(function (resolve, reject) {
        self.connect(function (err) {
            if (err)reject(err);
            if (self.transaction) {
                self.transaction.rollback(function (err) {
                    if (err) {
                        reject('事务回滚失败,因为事务过期或没有开始');
                    }
                    self.disconnect();
                    self.transactioned = false;
                    resolve();
                });
            }
            else {
                reject('请调用transaction.start()开启事务');
            }
        });
    });
};

Transation.prototype.ISOLATION_LEVEL = {
    READ_UNCOMMITTED: 'READ_UNCOMMITTED', // 未提交读(隔离级别最低，可以读取到一个事务正在处理的数据，但事务还未提交，这种级别的读取叫做脏读)
    READ_COMMITTED: 'READ_COMMITTED', // 读提交(默认级别，不能脏读，不能读取事务正在处理没有提交的数据，但能修改)
    REPEATABLE_READ: 'REPEATABLE_READ',// 可重复读(不能读取事务正在处理的数据，也不能修改事务处理数据前的数据)
    SERIALIZABLE: 'SERIALIZABLE', // 可序列化(在可重复读的隔离级别上有了更加高级的隔离级别，升级锁，可序列化，不只锁定记录，而且将表结构锁定,最高事务隔离级别，只能看到事务处理之前的数据)
    SNAPSHOT: 'SNAPSHOT'    // 快照隔离(当读取数据时，可以保证读操作读取的行是事务开始时间可用的最后提交的版本,启用Snapshop隔离级别，需要先在数据库级上设置相关选项:alter database 数据库名  set allow_snapshop_isolation on)
};

module.exports = Transation;