/* Private brokers and forwarded sessions, shared by the Cluster and
 * Connections pages. Node facts come from the topology payload's per-node
 * `runtime` block, connection facts from the connections payload. */
(() => {
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
  const count = value => Number.isFinite(Number(value)) ? Math.max(0, Math.floor(Number(value))) : 0;

  const isPrivate = node => node?.runtime?.visibility === 'private';

  // Upstream sessions this public broker holds open to private owners.
  // Null for private brokers, which never forward.
  const upstreamSessions = node => isPrivate(node) ? null : count(node?.runtime?.upstream_sessions);

  // Forwarded sessions this broker serves for public brokers.
  const forwardedSessions = node => count(node?.runtime?.forwarded_sessions);

  const badge = node => isPrivate(node)
    ? '<span class="chip-plain" title="Clients never connect to this broker. Public brokers forward to it.">private</span>'
    : '';

  // Short tags for the diagram caption under each ring.
  function captionTags(node) {
    const tags = [];
    if (isPrivate(node)) tags.push('private');
    const upstream = upstreamSessions(node);
    if (upstream) tags.push(`${upstream} upstream`);
    return tags;
  }

  // Key-value rows for the broker list card. Rows that would say nothing
  // useful (zero forwarded sessions on a broker that serves none) are left out.
  function nodeRows(node) {
    const rows = [];
    const upstream = upstreamSessions(node);
    if (upstream !== null) {
      rows.push(['upstream sessions', String(upstream)]);
    }
    const forwarded = forwardedSessions(node);
    if (forwarded || isPrivate(node)) {
      rows.push(['forwarded sessions served', String(forwarded)]);
    }
    return rows.map(([key, value]) =>
      `<div class="node-kv"><span>${escape(key)}</span><b>${escape(value)}</b></div>`).join('');
  }

  // Link text for a node's admin address. A private broker's address is
  // internal and may not open from outside the cluster network.
  const adminLinkText = node => isPrivate(node) ? 'open its admin (internal address)' : 'open its admin';

  // The banner for a cluster with private brokers but no live public broker.
  function gatewayNotice(coordination) {
    if (!coordination?.no_live_public_broker) return '';
    return '<div class="quarantine-title">No public broker is live</div>'
      + '<p>This cluster has private brokers but no live public broker to forward for them. '
      + 'Clients get empty owner endpoints for partitions owned by private brokers, so they '
      + 'cannot reach those partitions until a public broker is live.</p>';
  }

  // "via node P for user U" for a forwarded session, "" otherwise.
  function forwardedVia(conn) {
    const forwarded = conn?.forwarded;
    if (!forwarded) return '';
    const node = String(forwarded.origin_node ?? '');
    const user = String(forwarded.acting_user ?? '');
    return user ? `via node ${node} for user ${user}` : `via node ${node}`;
  }

  window.FibrilForwarding = {
    isPrivate, upstreamSessions, forwardedSessions, badge, captionTags, nodeRows,
    adminLinkText, gatewayNotice, forwardedVia,
  };
})();
