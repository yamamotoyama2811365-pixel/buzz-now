/* i-mobile eCPM test: top + mid-content + dismissible overlay only. */
(function () {
  'use strict';
  var config = {"key":"corporate","host":"buzz-now-1.onrender.com","prefix":"/corporate/","pc":{"mid":596383,"top":{"asid":1944835,"elementid":"im-d3359a305e3843298606fa646370358f","width":728,"height":90},"mid1":{"asid":1945381,"elementid":"im-884142a5efd7410c888e1929bbb6f54b","width":300,"height":250},"mid2":{"asid":1945382,"elementid":"im-a8786be5b90f4d36afa031d2c2ff711e","width":300,"height":250},"bottom":{"asid":1944823,"elementid":"im-158851c14b0f4ab3935a3aab85be0d33","width":300,"height":250},"overlay":{"asid":1945383,"elementid":"im-d079a538b5ce49ab9fcac57e3c3fea74","width":728,"height":90}},"sp":{"mid":596413,"top":{"asid":1945376,"elementid":"im-ca4e66f5d4dc4350a8949b70cc3a3dd8","width":320,"height":50},"mid1":{"asid":1945377,"elementid":"im-e8a4088b2d0e468ba542d589a3003907","width":300,"height":250},"mid2":{"asid":1945378,"elementid":"im-08341b0222334fa9b1ca68433416e0f9","width":300,"height":250},"bottom":{"asid":1945379,"elementid":"im-49ba2ded14f94b1586e97650ece65994","width":300,"height":250},"overlay":{"asid":1945380,"elementid":"im-1d0a56f12e024a24a5418a4120edeede","width":320,"height":50}}};
  var script = document.currentScript;
  var managed = script && script.dataset.managed === 'react';
  var active = null;
  var dismissed = false;

  function eligible() {
    return location.hostname === config.host &&
      (!config.prefix || location.pathname.indexOf(config.prefix) === 0) &&
      !/^\/line-support(?:\/|$)/.test(location.pathname);
  }

  function mount() {
    if (!eligible() || active) return;
    var mobile = /iphone|ipad|ipod|android|mobile|windows phone|blackberry|opera mini|opera mobi/i.test(navigator.userAgent || '');
    var width = document.documentElement.clientWidth || window.innerWidth;
    if (width < (mobile ? 320 : 769)) return;
    var platform = mobile ? 'sp' : 'pc';
    var inventory = config[platform];
    var nodes = [];
    var previousPadding = document.body.style.paddingBottom;
    var cleanup = function () {
      nodes.forEach(function (node) { node.remove(); });
      document.body.style.paddingBottom = previousPadding;
      active = null;
    };
    active = cleanup;

    var style = document.createElement('style');
    style.textContent = '.sow-imobile{box-sizing:border-box;display:block;clear:both;grid-column:1/-1;flex:0 0 100%;text-align:center;margin:26px auto;padding:0;max-width:100%;border:0;background:transparent}.sow-imobile-label{font:11px/18px sans-serif;opacity:.65;margin:0 0 6px}.sow-imobile-slot{margin:0 auto;max-width:100%}.sow-imobile-overlay{position:fixed;left:0;right:0;bottom:0;z-index:99998;background:rgba(20,24,30,.92);color:white;text-align:center;padding:0 0 env(safe-area-inset-bottom);margin:0;box-sizing:border-box}.sow-imobile-overlay-head{height:28px;display:flex;align-items:center;justify-content:center;position:relative}.sow-imobile-close{position:absolute;right:8px;top:0;width:32px;height:28px;border:0;border-radius:3px;background:transparent;color:#fff;font:22px/28px sans-serif;cursor:pointer;padding:0}.sow-imobile-close:focus-visible{outline:2px solid white;outline-offset:-2px}@media print{.sow-imobile,.sow-imobile-overlay{display:none!important}}';
    document.head.appendChild(style);
    nodes.push(style);

    var main = document.querySelector('main') || document.body;
    var candidates = Array.from(main.querySelectorAll('section,article,h2')).filter(function (node) {
      return !node.closest('header,footer,nav,aside,form,a,button,label,[data-imobile-placement]') &&
        !node.closest('[hidden]') && node.getBoundingClientRect().height > 0 && node.getBoundingClientRect().width >= 300;
    });
    if (!candidates.length) candidates = Array.from(main.children).filter(function (node) {
      return !/^(HEADER|FOOTER|NAV|SCRIPT|STYLE|ASIDE|FORM)$/.test(node.tagName) && node.getBoundingClientRect().height > 0;
    });
    var mainBox = main.getBoundingClientRect();
    var desired = mainBox.top + mainBox.height * 0.5;
    var target = candidates.reduce(function (best, node) {
      return !best || Math.abs(node.getBoundingClientRect().top - desired) < Math.abs(best.getBoundingClientRect().top - desired) ? node : best;
    }, null);
    while (target && target.parentElement && target.parentElement !== main && target.parentElement.getBoundingClientRect().width < 300) target = target.parentElement;

    function requestAd(spec, host) {
      var slot = document.createElement('div');
      slot.className = 'sow-imobile-slot';
      slot.id = spec.elementid;
      slot.style.width = spec.width + 'px';
      slot.style.minHeight = spec.height + 'px';
      host.appendChild(slot);
      (window.adsbyimobile = window.adsbyimobile || []).push({pid:85420,mid:inventory.mid,asid:spec.asid,type:'banner',display:'inline',elementid:spec.elementid});
    }

    ['top','mid1'].forEach(function (position) {
      var spec = inventory[position];
      var aside = document.createElement('aside');
      aside.className = 'sow-imobile';
      aside.setAttribute('aria-label', '広告');
      aside.dataset.imobilePlacement = position;
      aside.dataset.imobilePlatform = platform;
      var label = document.createElement('div');
      label.className = 'sow-imobile-label';
      label.textContent = '広告';
      aside.appendChild(label);
      nodes.push(aside);
      if (position === 'top') {
        var header = document.querySelector('header');
        if (header) header.after(aside); else main.prepend(aside);
      } else if (target) {
        target.before(aside);
      } else {
        main.appendChild(aside);
      }
      requestAd(spec, aside);
    });

    if (!dismissed) {
      var overlay = document.createElement('aside');
      overlay.className = 'sow-imobile-overlay';
      overlay.setAttribute('aria-label', '追従広告');
      overlay.dataset.imobilePlacement = 'overlay';
      overlay.dataset.imobilePlatform = platform;
      var head = document.createElement('div');
      head.className = 'sow-imobile-overlay-head';
      var label = document.createElement('span');
      label.textContent = '広告';
      label.style.fontSize = '11px';
      var close = document.createElement('button');
      close.type = 'button';
      close.className = 'sow-imobile-close';
      close.textContent = '×';
      close.setAttribute('aria-label', '追従広告を閉じる');
      close.addEventListener('click', function () {
        dismissed = true;
        overlay.remove();
        document.body.style.paddingBottom = previousPadding;
      });
      head.append(label, close);
      overlay.appendChild(head);
      document.body.appendChild(overlay);
      nodes.push(overlay);
      var bottomNav = document.querySelector('.bottom-nav,.mobile-bottom-nav');
      var offset = bottomNav && getComputedStyle(bottomNav).position === 'fixed' ? bottomNav.getBoundingClientRect().height : 0;
      overlay.style.bottom = offset + 'px';
      document.body.style.paddingBottom = 'calc(' + (parseFloat(getComputedStyle(document.body).paddingBottom) + inventory.overlay.height + 28 + offset) + 'px + env(safe-area-inset-bottom))';
      requestAd(inventory.overlay, overlay);
    }

    if (!document.querySelector('script[src^="https://imp-adedge.i-mobile.co.jp/script/v1/spot.js"]')) {
      var loader = document.createElement('script');
      loader.async = true;
      loader.src = 'https://imp-adedge.i-mobile.co.jp/script/v1/spot.js?20220104';
      document.head.appendChild(loader);
    }
    return cleanup;
  }

  window.SowImobile = {mount:mount,cleanup:function(){if(active)active();},version:'20260917-ecpm1'};
  if (!managed) {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount, {once:true}); else mount();
  }
}());
