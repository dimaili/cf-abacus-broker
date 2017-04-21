'use strict';

const _ = require('underscore');
const memoize = _.memoize;

const urienv = require('abacus-urienv');

const uris = memoize(() => urienv({
  cf_api: 9882,
  uaa_server: 9883,
  provisioning: 9880,
  collector: 9080
}));

const resourceUsagePath = process.env.RESOURCE_USAGE_PATH
  || '/v1/metering/collected/usage';

const resourceProviderPrefix = (id = '') => 'abacus-rp-' + id;

module.exports.uris = uris;
module.exports.resourceProviderPrefix = resourceProviderPrefix;
module.exports.resourceUsagePath = resourceUsagePath;
