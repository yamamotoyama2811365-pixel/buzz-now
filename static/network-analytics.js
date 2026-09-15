/* Shared aggregate PV only: no cookies, storage, fingerprint or visitor identifier. */
(() => {
  'use strict';
  const script = document.currentScript;
  const site = script && script.dataset ? script.dataset.site : '';
  if (!site || window.__managedNetworkPageview || navigator.webdriver || navigator.doNotTrack === '1' || navigator.globalPrivacyControl) return;

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
    if (!host) return 'direct';
    if (host === location.hostname.toLowerCase()) return 'internal';
    if (host === 't.co' || host === 'x.com' || host.endsWith('.x.com') || host === 'twitter.com' || host.endsWith('.twitter.com')) return 'x';
    if (/^(www\.)?google\.(com|co\.jp)$/.test(host)) return 'google';
    if (host === 'search.yahoo.co.jp' || host === 'search.yahoo.com') return 'yahoo';
    if (host === 'bing.com' || host === 'www.bing.com') return 'bing';
    return 'referral';
  }

  function nonce() {
    if (crypto.randomUUID) return crypto.randomUUID();
    return Array.from(crypto.getRandomValues(new Uint32Array(4)), v => v.toString(16).padStart(8, '0')).join('');
  }

  function send() {
    if (document.visibilityState !== 'visible' || document.prerendering || window.__managedNetworkPageview) return;
    window.__managedNetworkPageview = true;
    const q = new URLSearchParams(location.search);
    const payload = {
      site,
      path: location.pathname,
      source: classify(),
      test: q.get('pv_test') === '1',
      nonce: nonce()
    };
    fetch('https://buzz-now-1.onrender.com/api/network-analytics/pageview', {
      method: 'POST',
      mode: 'no-cors',
      credentials: 'omit',
      referrerPolicy: 'strict-origin-when-cross-origin',
      keepalive: true,
      headers: {'Content-Type': 'text/plain'},
      body: JSON.stringify(payload)
    }).catch(() => {});
  }

  document.addEventListener('visibilitychange', send);
  document.addEventListener('prerenderingchange', send);
  send();
})();
