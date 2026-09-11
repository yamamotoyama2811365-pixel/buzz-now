const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const crypto = require('node:crypto').webcrypto;
const script = fs.readFileSync('static/visitor-analytics.js', 'utf8');
function run(referrer, search = '', navigator = {}) {
  const calls = [];
  const context = {window: {}, navigator, URL, URLSearchParams, crypto,
    location: {hostname: 'buzz-now-1.onrender.com', pathname: '/trend/example', search},
    document: {referrer, visibilityState: 'visible', prerendering: false, addEventListener() {}},
    fetch: (url, options) => {calls.push({url, options, payload: JSON.parse(options.body)}); return Promise.resolve();}};
  vm.runInNewContext(script, context);
  vm.runInNewContext(script, context);
  return calls;
}
const cases = [
  ['', '', 'direct'], ['https://t.co/private-link', '', 'x'], ['https://www.google.com/search?q=secret', '', 'google'],
  ['https://www.google.co.jp/url?q=secret', '', 'google'], ['https://search.yahoo.co.jp/search?p=secret', '', 'yahoo'],
  ['https://www.bing.com/search?q=secret', '', 'bing'], ['https://buzz-now-1.onrender.com/', '', 'internal'],
  ['https://other.example/private?secret=1', '', 'referral'], ['', '?utm_source=x&utm_medium=social', 'x'],
  ['', '?gclid=secret', 'paid'], ['', '?utm_source=google&utm_medium=organic', 'google'],
  ['https://google.com.evil.example/?secret=1', '', 'referral'], ['', '?from=x', 'x']
];
for (const [ref, search, expected] of cases) {
  const calls = run(ref, search);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].payload.source, expected);
  assert.equal(calls[0].options.mode, 'cors');
  assert.equal(calls[0].options.credentials, 'omit');
  assert.equal(calls[0].options.referrerPolicy, 'no-referrer');
  assert.equal(calls[0].options.body.includes('secret'), false);
}
assert.equal(run('', '', {webdriver: true}).length, 0);
assert.equal(run('', '', {doNotTrack: '1'}).length, 0);
assert.equal(run('', '', {globalPrivacyControl: true}).length, 0);
assert.equal(run('', '?bn_analytics_test=1')[0].payload.test, true);
assert.equal(run('')[0].payload.test, false);
console.log('18 browser analytics cases passed');
