'use strict';

const _ = require('underscore');
const memoize = _.memoize;
const uniq = _.uniq;
const range = _.range;
const map = _.map;
const filter = _.filter;
const flatten = _.flatten;
const reduce = _.reduce;
const extend = _.extend;

const batch = require('abacus-batch');
const breaker = require('abacus-breaker');
const cluster = require('abacus-cluster');
const dataflow = require('abacus-dataflow');
const dbclient = require('abacus-dbclient');
const mconfigcb = require('abacus-metering-config');
const moment = require('abacus-moment');
const oauth = require('abacus-oauth');
const request = require('abacus-request');
const retry = require('abacus-retry');
const router = require('abacus-router');
const seqid = require('abacus-seqid');
const throttle = require('abacus-throttle');
const timewindow = require('abacus-timewindow');
const transform = require('abacus-transform');
const urienv = require('abacus-urienv');
const webapp = require('abacus-webapp');
const yieldable = require('abacus-yieldable');

const tmap = yieldable(transform.map);

const brequest = yieldable(retry(breaker(batch(request))));
// const brequest = yieldable(retry(breaker(request)));

const mconfig = yieldable(mconfigcb);

// Setup debug log
const debug = require('abacus-debug')('abacus-housekeeper');
const edebug = require('abacus-debug')('e-abacus-housekeeper');

const dbaliasCollector = process.env.DBALIAS_COLLECTOR || 'db';
const dbaliasMeter = process.env.DBALIAS_METER || 'db';
const dbaliasAccumulator = process.env.DBALIAS_ACCUMULATOR || 'db';
const dbaliasAggregator = process.env.DBALIAS_AGGREGATOR || 'db';
const dbaliasBridge = process.env.DBALIAS_BRIDGE || 'db';
const dbaliasPlugins = process.env.DBALIAS_PLUGINS || 'db';

// Resolve service URIs
const uris = memoize(() => urienv({
  auth_server : 9882,
  [dbaliasCollector] : 5984,
  [dbaliasMeter] : 5984,
  [dbaliasAccumulator] : 5984,
  [dbaliasAggregator] : 5984,
  [dbaliasBridge] : 5984,
  [dbaliasPlugins] : 5984
}));

// Function call statistics
const statistics = {
  tasks: {
    deleteOldPartitions: {
      deletedPartitionsCount: 0,
      errors: 0
    }
  },
  retries: {
    count: 0
  }
};

const errors = {
  deleteOldPartitions: null
};

// Enable tasks 
const enabledTasks = (process.env.ENABLED_TASKS || '').split(',');

// Retention period
const retentionPeriod = parseInt(process.env.RETENTION_PERIOD) || 3;

const pad = (num) => {
  return (num > 9 ? '' : '0') + num;
};

const toString = (arr) => {
  if (arr.length === 0)
    return '';
  const s = reduce(arr.slice(1), (acc, v) => acc + '|' + pad(v), pad(arr[0]));
  return arr.length === 1 ? s : '(' + s + ')';
};

// Create a regex that matches <year><month> partition suffixes for
// years / months older than the specified moment, starting with 2015.
// For example, the following regex matches all suffixes older than 201606.
// .*-20(15(0[1-9]|1[0-2])|16(01|02|03|04|05|06))
const regex = (m) => {
  const year = m.year() - 2000;
  const pastYears = toString(range(15, year));
  const years = toString(range(15, year + 1));
  const allMonths = '(0[1-9]|1[0-2])';
  const months = toString(map(range(0, m.month() + 1), (x) => x + 1));
  const s = 
    m.year() === 2015 ? '.*-20' + pad(year) + months :
    m.month() === 11 ? '.*-20' + years + allMonths :
    '.*-20(' + pastYears + allMonths + '|' + pad(year) + months + ')';
  return new RegExp(s);
};

// Database servers
const servers = uniq([]
  .concat(uris()[dbaliasCollector])
  .concat(uris()[dbaliasMeter])
  .concat(uris()[dbaliasAccumulator])
  .concat(uris()[dbaliasAggregator])
  .concat(uris()[dbaliasBridge])
  .concat(uris()[dbaliasPlugins]));

// Delete all partitions on all servers older than the configured retention
// period
const deleteOldPartitions = (cb) => {
  if (!enabledTasks.includes('DELETE_OLD_PARTITIONS'))
    return cb();

  const last = moment.utc().startOf('month')
    .subtract(retentionPeriod + 1, 'months');
  debug('Deleting partitions on servers %o older than %s', servers,
    '' + last.year() + pad(last.month() + 1));
  return dbclient.deletePartitions(servers, regex(last), (err, res) => {
    if (err) {
      statistics.tasks.deleteOldPartitions.errors++;
      errors.deleteOldPartitions = err;
      return cb(err);
    }

    statistics.tasks.deleteOldPartitions.deletedPartitionsCount += res.length;
    return cb();
  });
};

const orgsUrl = process.env.ORGS_URL || 'http://localhost:8080/orgs';
const spacesUrl = process.env.SPACES_URL || 'http://localhost:8080/spaces';

// Time dimensions
const dimensions = ['s', 'm', 'h', 'D', 'M'];

const aggregatorInputDb = dataflow.db('abacus-aggregator-accumulated-usage',
  undefined, uris()[dbaliasAggregator]);
const aggregatorOutputDb = dataflow.db('abacus-aggregator-aggregated-usage',
  undefined, uris()[dbaliasAggregator]);

// TODO: The following code until orgUsage() is largely copy / pasted from 
// abacus-usage-reporting. This code should be rather refactored to a 
// separate module and reused.

// Return the summarize function for a given metric
const summarizefn = (metrics, metric) => {
  return filter(metrics, (m) => m.name === metric)[0].summarizefn;
};

// get metering plan
const getMeteringPlan = function *(id, auth) {
  const mplan = yield mconfig(id, auth);

  // Error when getting rating plan
  if(mplan.error) {
    debug('Error when getting plan ' + id + '. ' +
      mplan.reason);

    throw extend({
      statusCode: 200
    }, mplan);
  };

  return mplan.metering_plan;
};

// Summarize a metric
const summarizeMetric = (m, t, processed, sfn) => {
  // Clone the metric and extend with a usage summary
  return extend({}, m, {
    windows: map(m.windows, (w, i) => {
      return map(w, (wi, j) => {
        const bounds = timewindow.timeWindowBounds(
          processed, dimensions[i], -j);
        return wi ? sfn ? extend({}, wi, {
          summary: sfn(t, wi.quantity, bounds.from, bounds.to)
        }) : extend({}, wi) : null;
      });
    })
  });
};

// Compute usage summaries for the given aggregated usage
const summarizeUsage = function *(t, a) {
  debug('Summarizing usage for time %o and aggregated usage %o', t, a);

  // Summarize the aggregated usage under a resource
  const summarizeResource = function *(rs) {
    // Clone the resource and extend it with usage summaries
    return extend({}, rs, {
      plans: yield tmap(rs.plans, function *(p) {
        // Find the metrics configured for the given metering plan
        const mplan = yield getMeteringPlan(p.metering_plan_id, 
          undefined); // TODO: Authentication

        return extend({}, p, {
          aggregated_usage: map(p.aggregated_usage, (m) => {
            return summarizeMetric(m, t, a.processed,
              summarizefn(mplan.metrics, m.metric));
          })
        });
      })
    });
  };

  // Clone the aggregated usage and extend it with usage summaries
  const s = extend({}, a, {
    resources: yield tmap(a.resources, summarizeResource),
    spaces: yield tmap(a.spaces, function *(space) {
      return extend({}, space, {
        resources: yield tmap(space.resources, summarizeResource)
        // TODO: Populate consumers
      });
    })
  });
  debug('Summarized usage %o', s);
  return s;
};

// Return the usage for an org in a given time period
const orgUsage = function *(orgid, time) {
  // Compute the query range
  const t = time || moment.now();
  const d = moment.utc(t);
  const mt = moment.utc([d.year(), d.month(), 1]).valueOf();
  const sid = dbclient.kturi(orgid, seqid.pad16(t)) + 'ZZZ';
  const eid = dbclient.kturi(orgid, seqid.pad16(mt));

  debug('Retrieving latest rated usage between %s and %s', eid, sid);
  const doc = yield aggregatorOutputDb.allDocs({
    endkey: eid,
    startkey: sid,
    descending: true,
    limit: 1,
    include_docs: true
  });
  if (!doc.rows.length)
    throw 'Error'; // TODO
  debug('Retrieved rated usage %o', doc.rows[0].doc);

  // TODO: Popluate consumer usage (see abacus-usage-reporting)
  return yield summarizeUsage(t, doc.rows[0].doc);
};

// Send reports to metering hub api
const sendReports = function *(reports, url) {
  yield tmap(reports, function *(report) {
    debug('Sending POST request to metering hub');
    const res = yield brequest.post(url, {
      headers: { 'Content-Type': 'application/json' },
      body: report
    });
    debug('Received response %s from metering hub', res.statusCode);

    if (res.statusCode !== 204) {
      edebug('Unable to post usage reports to metering hub, %s: %o',
        res.statusCode, res.body);

      // Throw response object as an exception to stop further processing
      throw res;
    }
  });
};

const postMeteringHub = function *() {
  if (!enabledTasks.includes('POST_METERING_HUB'))
    return;

  // Get all organizations from aggregator db
  const start = moment.utc().startOf('month').valueOf();
  const end = moment.utc().endOf('month').valueOf();
  debug('Getting organizations between %s and %s', start, end);
  const orgs = yield aggregatorInputDb.distinctValues('organization_id', {
    startkey: 't/000' + start,
    endkey: 't/000' + end
  });
  debug('Organizations: %o', orgs);

  // Get organization, space, and consumer reports
  const time = moment.utc().endOf('hour').valueOf();
  yield tmap(orgs, function *(org) {
    debug('Getting aggregated usage for org %s and time %s', org, time);
    const doc = yield orgUsage(org, time);

    // Generate org reports
    const orgReports = flatten(map(doc.resources, (resource) => 
      map(resource.plans, (plan) => ({
        org: org,
        resource: resource.resource_id,
        plan: plan.plan_id,
        timestamp: time,
        usage: map(plan.aggregated_usage, (usage) => ({
          metric: usage.metric,
          day: usage.windows[3][0].summary,
          month: usage.windows[4][0].summary
        }))
      }))));
    debug('Org reports: %o', orgReports);

    // Send org reports to metering hub
    yield sendReports(orgReports, orgsUrl);

    // Generate space reports
    const spaceReports = flatten(map(doc.spaces, (space) => 
      map(space.resources, (resource) => 
        map(resource.plans, (plan) => ({
          org: org,
          space: space.space_id,
          resource: resource.resource_id,
          plan: plan.plan_id,
          timestamp: time,
          usage: map(plan.aggregated_usage, (usage) => ({
            metric: usage.metric,
            day: usage.windows[3][0].summary,
            month: usage.windows[4][0].summary
          }))
        })))));
    debug('Space reports: %o', spaceReports);

    // Send space reports to metering hub
    yield sendReports(spaceReports, spacesUrl);

    // TODO: Generate cosumer reports and send them to metering hub
  });
};

const tasks = [deleteOldPartitions, yieldable.functioncb(postMeteringHub)];

// Use secure routes or not
const secured = process.env.SECURED === 'true';

// Retry interval
const retryInterval = parseInt(process.env.RETRY_INTERVAL) || 86400000;

// Create an express router
const routes = router();

const setHousekeepTimeout = (fn, interval) => {
  clearTimeout(module.housekeeper);
  module.housekeeper = setTimeout(fn, interval);
  debug('Retry interval set to %d ms after %d retries',
    interval, statistics.retries.count);
  statistics.retries.count++;
};

const runTasks = (cb) => {
  debug('Running housekeeper tasks');

  debug('Scheduling next execution on %s',
    moment.utc().add(retryInterval, 'milliseconds').toDate());
  setHousekeepTimeout(() => runTasks(cb), retryInterval);

  transform.map(tasks, (task, index, tasks, mcb) => task(mcb), cb);
};

const stopHousekeeper = (cb = () => {}) => {
  edebug('Stopping housekeeper');

  // Cancel scheduled timers
  clearTimeout(module.housekeeper);

  if (typeof cb === 'function')
    cb();
};

const startHousekeeper = () => {
  debug('Starting housekeeper');

  setHousekeepTimeout(() => runTasks((err) => {
    if (err)
      edebug('Housekeeper tasks failed');
    else
      debug('Housekeeper tasks completed successfully');
  }), 0);

  process.on('exit', stopHousekeeper);
};

routes.get('/v1/housekeeper', throttle(function *(req) {
  return {
    body: {
      housekeeper: {
        statistics: statistics,
        errors: errors
      }
    }
  };
}));

// Create a housekeeper app
const housekeep = () => {
  debug('Starting housekeeper app');
  cluster.singleton();

  if (cluster.isWorker()) {
    debug('Starting housekeeper worker');
    startHousekeeper();
  }

  // Create the Webapp
  const app = webapp();

  if(secured)
    app.use(routes, oauth.validator(process.env.JWTKEY, process.env.JWTALGO));
  else
    app.use(routes);

  return app;
};

// Command line interface, create the housekeeper app and listen
const runCLI = () => housekeep().listen();

module.exports = housekeep;
module.exports.regex = regex;
module.exports.runTasks = runTasks;
module.exports.runCLI = runCLI;
