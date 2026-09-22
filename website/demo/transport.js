(() => {
  'use strict';
  const originalFetch = window.fetch.bind(window);
  const root = '/dashboard-demo/';
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
    if (url.pathname.startsWith(root) && (page.test(resource) || resource.startsWith('static/'))) return originalFetch(input, init);
    return reply({ error: 'Request outside the static demo.' }, 403);
  };

  const mutationSelector = [
    '#create-toggle', '[id="create.submit"]', '.queue-delete-btn', '#q-test-send',
    '[data-test-publish]', '#message-replay', '.quarantine-repair', '#drain-btn',
    '#repartition-submit', '#membership-add', '#membership-remove', '#tls-reload',
    '#user-save', '.user-del', '.user-delete',
    '#settings-form button[type="submit"]', '#user-form button[type="submit"]',
    '#global-dlq-form button[type="submit"]', '#queue-dlq-form button[type="submit"]',
  ].join(',');
  const protect = () => {
    document.querySelectorAll(mutationSelector).forEach(el => {
      if (!el.disabled) el.disabled = true;
      if (el.title !== 'Unavailable in the read-only demo') el.title = 'Unavailable in the read-only demo';
    });
    // This dashboard is a fixed example, so a green network health pill would
    // imply a live broker. Keep the shell while labeling its actual data mode.
    const pill = document.getElementById('live-pill-text');
    if (pill && pill.textContent !== 'Sample data') pill.textContent = 'Sample data';
  };
  document.addEventListener('click', event => {
    if (event.target.closest(mutationSelector)) { event.preventDefault(); event.stopImmediatePropagation(); }
  }, true);
  document.addEventListener('submit', event => {
    if (event.target.id !== 'message-inspection-form') { event.preventDefault(); event.stopImmediatePropagation(); }
  }, true);
  document.addEventListener('DOMContentLoaded', () => {
    protect();
    new MutationObserver(protect).observe(document, { childList: true, subtree: true, attributes: true, attributeFilter: ['disabled'] });
  });
})();
