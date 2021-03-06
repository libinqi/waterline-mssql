'use strict';

/**
 * Functions
 * Created by libinqi on 2016/5/10.
 */

var _ = require('lodash');

var Query = function (schema) {
  this._schema = _.clone(schema);
  return this;
};

/**
 * Cast special values to proper types.
 *
 * Ex: Array is stored as "[0,1,2,3]" and should be cast to proper
 * array for return values.
 */

Query.prototype.cast = function (values) {
  var _values = _.clone(values);
  var self = this;

  Object.keys(values).forEach(function (key) {
    if (!self._schema[key]) {
      return;
    }

    // Lookup schema type
    var type = self._schema[key].type;

    if (!type) {
      return;
    }

    // Attempt to parse Array or JSON type
    if (type === 'array' || type === 'json') {
      try {
        _values[key] = JSON.parse(values[key]);
      } catch (e) {
        return;
      }
    }

    // Convert booleans back to true/false
    if (type === 'boolean') {
      var val = values[key];

      if (val === 0) {
        _values[key] = false;
      }

      if (val === 1) {
        _values[key] = true;
      }
    }

    if (type === 'binary') {
      _values[key] = new Buffer(_values[key] || []);
    }

  });

  return _values;
};

module.exports = Query;
