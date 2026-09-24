(() => {
  'use strict';
  const originalFetch = window.fetch.bind(window);
  const root = '/dashboard-demo/';
  const embedded = new URL(location.href).searchParams.get('embed') === '1';
  if (embedded) document.documentElement.dataset.demoEmbed = 'true';
  // A finite panel choice keeps documentation focus predictable. The full demo
  // remains the complete production page, including its ordinary navigation.
  if (embedded && /\/admin\/topology\/?$/.test(location.pathname)
      && new URL(location.href).searchParams.get('panel') === 'recovery') {
    document.documentElement.dataset.demoPanel = 'recovery';
  }
  // Exercising the normal polling path avoids emulating broker SSE state.
  window.EventSource = undefined;
  window.fetch = async (input, init = {}) => {
    const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url, location.href);
    const method = (init.method || input.method || 'GET').toUpperCase();
    const reply = (data, status = 200) => new Response(JSON.stringify(data), {
      status, headers: { 'Content-Type': 'application/json' },
    });
    if (method !== 'GET' && method !== 'HEAD') return reply({ error: 'This demo is read-only.' }, 403);
    if (url.origin !== location.origin) return reply({ error: 'External requests are disabled in this demo.' }, 403);
    if (url.pathname.startsWith('/admin/api/')) {
      const result = window.fibrilDemoResponse(url);
      if (result === undefined) {
        console.error(`Missing dashboard demo fixture: ${url.pathname}`);
        return reply({ error: 'No sample data for this endpoint.' }, 404);
      }
      return reply(result);
    }
    if (url.pathname === '/healthz') return reply({ status: 'demo' });
    // Only static demo resources/navigation may reach the server. There is no
    // fallback to a live admin API, including for unknown paths or mutations.
    const page = /^(?:index\.html|admin\/(?:queues|queue|streams|messages|dlq|connections|subscriptions|activity|topology|diagnostics|security|settings)\/?(?:index\.html)?)?$/;
    const resource = url.pathname.slice(root.length);
    if (url.pathname.startsWith(root) && ((!embedded && page.test(resource)) || resource.startsWith('static/'))) return originalFetch(input, init);
    return reply({ error: 'Request outside the static demo.' }, 403);
  };

  const mutationSelector = [
    '#create-toggle', '[id="create.submit"]', '.queue-delete-btn', '#q-test-send',
    '[data-test-publish]', '#message-replay', '.quarantine-repair', '#drain-btn',
    '#repartition-submit', '#membership-add', '#membership-remove', '#tls-reload',
    '#user-save', '.user-del', '.user-delete',
    '#settings-form button[type="submit"]', '#local-storage-form button[type="submit"]', '#user-form button[type="submit"]',
    '#global-dlq-form button[type="submit"]', '#queue-dlq-form button[type="submit"]',
  ].join(',');
  const protect = () => {
    if (embedded) {
      // Removing href also closes keyboard/context-menu navigation. Keep the
      // text so the shared view still explains where the real dashboard links.
      document.querySelectorAll('a[href]').forEach(el => {
        el.dataset.demoHref = el.getAttribute('href');
        el.removeAttribute('href');
        el.setAttribute('aria-disabled', 'true');
        el.setAttribute('tabindex', '-1');
        el.title = 'Open the full dashboard above to browse other views';
      });
    }
    document.querySelectorAll(mutationSelector).forEach(el => {
      if (!el.disabled) el.disabled = true;
      if (el.title !== 'Unavailable in the read-only demo') el.title = 'Unavailable in the read-only demo';
    });
    // This dashboard is a fixed example, so a green network health pill would
    // imply a live broker. Keep the shell while labeling its actual data mode.
    const pill = document.getElementById('live-pill-text');
    if (pill && pill.textContent !== 'Sample data') pill.textContent = 'Sample data';
  };
  for (const type of ['click', 'auxclick']) document.addEventListener(type, event => {
    if (event.target.closest(mutationSelector) || (embedded && event.target.closest('a'))) {
      event.preventDefault(); event.stopImmediatePropagation();
    }
  }, true);
  document.addEventListener('keydown', event => {
    // The production palette has its own document-level keyboard handler.
    if (embedded && (event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault(); event.stopImmediatePropagation();
    }
  }, true);
  document.addEventListener('submit', event => {
    if (event.target.id !== 'message-inspection-form') { event.preventDefault(); event.stopImmediatePropagation(); }
  }, true);
  document.addEventListener('DOMContentLoaded', () => {
    // A lazy frame can be discarded while its loading callbacks are queued.
    const element = document?.documentElement;
    if (!element) return;
    protect();
    const observer = new MutationObserver(protect);
    const observe = () => observer.observe(element, { childList: true, subtree: true, attributes: true, attributeFilter: ['disabled', 'href'] });
    observe();
    window.addEventListener('pagehide', () => observer.disconnect());
    window.addEventListener('pageshow', event => { if (event.persisted) { protect(); observe(); } });
  });
})();
