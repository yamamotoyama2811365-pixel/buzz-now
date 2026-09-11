/* Aggregate PV only: no cookies, storage, fingerprint or visitor identifier. */
(() => {
  'use strict';
  if (window.__buzzNowPageview || navigator.webdriver || navigator.doNotTrack === '1' || navigator.globalPrivacyControl) return;
  function classify() {
    const q = new URLSearchParams(location.search);
    const campaign = (q.get('utm_source') || '').toLowerCase();
    const medium = (q.get('utm_medium') || '').toLowerCase();
    if (q.has('gclid') || q.has('dclid') || q.has('msclkid') || /^(cpc|ppc|paid|paidsearch|paid_social)$/.test(medium)) return 'paid';
    const hint = (q.get('from') || q.get('src') || '').toLowerCase();
    if (['x', 'twitter', 't.co'].includes(campaign) || ['x', 'twitter'].includes(hint)) return 'x';
    if (['google', 'yahoo', 'bing'].includes(campaign) && medium === 'organic') return campaign;
    let host = '';
    try { host = new URL(document.referrer).hostname.toLowerCase(); } catch (_) { return 'direct'; }
    if (host === location.hostname.toLowerCase() || ['buzz-now.onrender.com', 'buzz-now-1.onrender.com'].includes(host)) return 'internal';
    if (host === 't.co' || host === 'x.com' || host.endsWith('.x.com') || host === 'twitter.com' || host.endsWith('.twitter.com')) return 'x';
    if (/^(www\.)?google\.(com|co\.jp)$/.test(host)) return 'google';
    if (host === 'search.yahoo.co.jp' || host === 'search.yahoo.com') return 'yahoo';
    if (host === 'bing.com' || host === 'www.bing.com') return 'bing';
    return 'referral';
  }
  function send() {
    if (document.visibilityState !== 'visible' || document.prerendering || window.__buzzNowPageview) return;
    window.__buzzNowPageview = true;
    const q = new URLSearchParams(location.search);
    const nonce = crypto.randomUUID ? crypto.randomUUID() : Array.from(crypto.getRandomValues(new Uint32Array(4)), v => v.toString(16).padStart(8, '0')).join('');
    fetch('/api/visitor-analytics/pageview', {
      method: 'POST', mode: 'same-origin', credentials: 'omit', referrerPolicy: 'no-referrer', keepalive: true,
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({path: location.pathname, source: classify(), test: q.get('bn_analytics_test') === '1', nonce})
    }).catch(() => {});
  }
  document.addEventListener('visibilitychange', send);
  document.addEventListener('prerenderingchange', send);
  send();
})();
