// AutoKB Web UI client
(function() {
  'use strict';

  const API = '/api';
  const $ = (id) => document.getElementById(id);
  const escapeHtml = (s) => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  // ---- x-enum-source fetch cache ----
  const enumSourceCache = new Map();

  // ---- Enum search filter (used by inline oninput handlers) ----
  window.filterEnumSelect = function(input, selectName) {
    const select = input.parentElement.querySelector('select[name="' + selectName + '"]');
    if (!select) return;
    const query = input.value.toLowerCase();
    const optgroups = select.querySelectorAll('optgroup');
    if (optgroups.length === 0) return;
    // Cache original option data on first call
    const cacheKey = '_filterCache_' + selectName;
    if (!select[cacheKey]) {
      select[cacheKey] = [];
      for (const og of optgroups) {
        const groupOpts = [];
        for (const opt of og.querySelectorAll('option')) {
          groupOpts.push({ value: opt.value, text: opt.textContent });
        }
        select[cacheKey].push({ label: og.getAttribute('label') || '', options: groupOpts });
      }
    }
    const currentValue = select.value;
    const cache = select[cacheKey];
    for (let i = 0; i < optgroups.length && i < cache.length; i++) {
      const og = optgroups[i];
      const groupData = cache[i];
      let html = '';
      let visibleCount = 0;
      for (const optData of groupData.options) {
        if (optData.text.toLowerCase().includes(query)) {
          const sel = optData.value === currentValue ? ' selected' : '';
          html += '<option value="' + optData.value + '"' + sel + '>' + optData.text + '</option>';
          visibleCount++;
        }
      }
      if (visibleCount > 0) {
        og.innerHTML = html;
        og.style.display = '';
        og.disabled = false;
      } else {
        og.innerHTML = '';
        og.style.display = 'none';
        og.disabled = true;
      }
    }
  };

  // ---- Auth bootstrap ----
  async function checkAuth() {
    try {
      const r = await fetch('/auth/check');
      if (r.ok) {
        const data = await r.json();
        $('username-display').textContent = data.username || '';
        $('login-view').style.display = 'none';
        $('app-view').style.display = 'block';
        return true;
      }
    } catch (e) { /* fall through */ }
    $('login-view').style.display = 'flex';
    $('app-view').style.display = 'none';
    return false;
  }

  $('login-form').addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const u = $('login-username').value;
    const p = $('login-password').value;
    const r = await fetch('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: u, password: p }),
    });
    if (r.ok) {
      $('login-error').style.display = 'none';
      // Server-side redirect to / (auth-gated; serves the SPA shell which
      // then renders the dashboard view).
      window.location.href = '/';
    } else {
      const data = await r.json();
      $('login-error').textContent = data.error || 'Login failed';
      $('login-error').style.display = 'block';
    }
  });

  $('logout-btn').addEventListener('click', async () => {
    await fetch('/auth/logout', { method: 'POST' });
    location.hash = '';
    location.reload();
  });

  // ---- API helpers ----
  async function api(path, opts = {}) {
    const r = await fetch(API + path, {
      credentials: 'include',
      headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
      ...opts,
    });
    if (!r.ok) {
      const text = await r.text();
      let detail = text;
      try {
        const parsed = JSON.parse(text);
        if (parsed && parsed.detail) detail = parsed.detail;
      } catch (e) { /* not JSON — keep raw body */ }
      throw new Error(detail);
    }
    if (r.status === 204) return null;
    return r.json();
  }

  // ---- Routing ----
  let currentView = 'dashboard';
  let currentPluginId = null;
  let currentSinkId = null;
  let currentSink = null;
  let subscriptionsCache = {};
  let sseSource = null;
  let devlabEditPluginId = null;

  function navigate(view, params = {}) {
    currentView = view;
    if (view === 'dashboard') {
      showOnly('view-dashboard');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.toggle('active', el.dataset.view === 'dashboard'));
      loadDashboard();
    } else if (view === 'data-sources') {
      showOnly('view-data-sources');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.toggle('active', el.dataset.view === 'data-sources'));
      loadPlugins();
    } else if (view === 'all-subscriptions') {
      showOnly('view-subscriptions-all');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.toggle('active', el.dataset.view === 'all-subscriptions'));
      loadAllSubscriptions();
    } else if (view === 'subscriptions') {
      currentPluginId = params.plugin_id;
      showOnly('view-subscriptions');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.remove('active'));
      loadSubscriptions(params.plugin_id);
    } else if (view === 'activity') {
      showOnly('view-activity');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.toggle('active', el.dataset.view === 'activity'));
      loadActivity();
    } else if (view === 'data-destinations') {
      showOnly('view-data-destinations');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.toggle('active', el.dataset.view === 'data-destinations'));
      loadSinks();
    } else if (view === 'destinations-detail') {
      currentSinkId = params.service_id;
      showOnly('view-destinations-detail');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.remove('active'));
      loadTargets(params.service_id);
    } else if (view === 'data-targets') {
      showOnly('view-data-targets');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.toggle('active', el.dataset.view === 'data-targets'));
      loadAllTargets();
    } else if (view === 'devlab') {
      showOnly('view-devlab');
      document.querySelectorAll('.nav-link').forEach(el => el.classList.toggle('active', el.dataset.view === 'devlab'));
      // Developer Lab landing (#/devlab) shows two tiles: Source Developer
      // (#/devlab/source) and Destination Developer (#/devlab/destination). Legacy
      // #/devlab/source?edit={plugin_id} (from the Edit Source button) maps to the
      // source lab in edit mode. See DesignSpecification §7.9.
      const sub = params.sub || (params.edit ? 'source' : null);
      if (sub === 'destination') {
        showDevlabPanel('destination');
        if (params.edit) {
          loadSinkDevlabForEdit(params.edit);
        } else {
          resetSinkDevlabToCreateMode();
        }
        loadSinkGuide();
      } else if (sub === 'source') {
        showDevlabPanel('source');
        if (params.edit) {
          loadDevlabForEdit(params.edit);
        } else {
          resetDevlabToCreateMode();
        }
        loadPluginGuide();
      } else {
        showDevlabLanding();
      }
    }
    // Dashboard has its own 30s health timer; clear it on any navigation
    // away from the dashboard so the timer doesn't fire while the user is
    // on a different page. Re-armed on return by loadDashboard().
    if (view !== 'dashboard') {
      stopDashboardHealthTimer();
    }
  }

  function showOnly(viewId) {
    ['view-dashboard', 'view-data-sources', 'view-subscriptions-all', 'view-subscriptions', 'view-activity', 'view-event-detail', 'view-devlab', 'view-data-destinations', 'view-destinations-detail', 'view-data-targets'].forEach(id => {
      $(id).style.display = (id === viewId) ? 'block' : 'none';
    });
  }

  // The plugin-detail header pushes the title (h2) right of the Back
  // button by an amount equal to the Back button's own width, so the
  // visual rhythm matches the buttons on the right. Measure the Back
  // button each time the view is shown (text can vary by language /
  // styling) and on window resize.
  function syncBackButtonSpacing() {
    const back = $('back-to-dashboard');
    const header = back && back.closest('.sub-header');
    if (back && header) {
      header.style.setProperty('--back-button-width', `${back.offsetWidth}px`);
    }
  }
  window.addEventListener('resize', syncBackButtonSpacing);

  window.addEventListener('hashchange', () => parseHash());
  function parseHash() {
    const h = location.hash.replace(/^#\/?/, '');
    if (!h) return navigate('dashboard');
    // The view segment may carry a query string (e.g. ``devlab?edit=foo``)
    // because the canonical form is ``#/devlab?edit=foo`` and a naive
    // split('/') leaves ``?edit=foo`` glued to the view name. Strip the
    // query off the view segment first so downstream checks like
    // ``view === 'devlab'`` succeed; carry the query into ``rest[0]``
    // so the existing devlab-query parser below still finds it.
    const [viewRaw, ...rest] = h.split('/');
    const qIdx = viewRaw.indexOf('?');
    const view = qIdx >= 0 ? viewRaw.slice(0, qIdx) : viewRaw;
    const viewQuery = qIdx >= 0 ? viewRaw.slice(qIdx) : null;
    const restWithQuery = viewQuery ? [viewQuery, ...rest] : rest;
    const params = {};
    if (view === 'subscriptions' && restWithQuery[0]) {
      // Strip any trailing query string (?cache_bust, etc.) — a naive
      // split('/') leaves "?cb=3" attached to the plugin id and breaks
      // the schema fetch (which 200s with the wrong shape).
      params.plugin_id = restWithQuery[0].split('?')[0];
    } else if (view === 'destinations-detail' && restWithQuery[0]) {
      params.service_id = restWithQuery[0].split('?')[0];
    } else if (view === 'data-destinations' || view === 'data-targets') {
      // no params needed
    }
    if (view === 'devlab') {
      // Sub-routes: #/devlab/source and #/devlab/destination (optionally with a
      // glued ?edit= query). Legacy #/devlab?edit={plugin_id} (from the
      // Edit Source button) still parses as a source-lab edit.
      let sub = null;
      let qs = viewQuery;
      if (rest[0]) {
        const m = rest[0].match(/^(source|destination)(\?.*)?$/);
        if (m) {
          sub = m[1];
          qs = m[2] || rest[1] || null;
        }
      }
      if (sub) params.sub = sub;
      if (qs) {
        let str = qs;
        if (str.startsWith('?')) str = str.slice(1);
        for (const pair of str.split('&')) {
          const [k, v] = pair.split('=');
          if (k && v) params[k] = decodeURIComponent(v);
        }
      }
    }
    navigate(view, params);
  }

  $('back-to-dashboard').addEventListener('click', () => { location.hash = '#/data-sources'; });
  $('dest-back-to-list').addEventListener('click', () => { location.hash = '#/data-destinations'; });

  // ---- Plugins (Data Sources) ----
  async function loadPlugins() {
    try {
      const plugins = await api('/plugins');
      const grid = $('plugin-grid');
      grid.innerHTML = '';
      // Cache the count for the dashboard's "Total Plugins" stat so we
      // don't have to fire a second /api/plugins fetch from loadDashboard.
      lastPluginsCount = plugins.length;
      for (const p of plugins) {
        const card = document.createElement('div');
        card.className = 'plugin-card';
        card.innerHTML = `
          <img src="/assets/${escapeHtml(p.icon)}" onerror="this.src='/assets/default_icon.png'" alt="" />
          <div>
            <div class="plugin-name">${escapeHtml(p.display_name || p.name)}</div>
            <span class="badge badge-${escapeHtml(p.sub_type)}">${p.sub_type === 'EVENT_BASED' ? 'EVENT-BASED' : escapeHtml(p.sub_type)}</span>
          </div>
        `;
        card.addEventListener('click', () => { location.hash = `#/subscriptions/${p.plugin_id}`; });
        grid.appendChild(card);
      }
    } catch (e) { console.error(e); }
  }

  // ---- Dashboard ----
  // In-memory values feed the stats panel:
  //   - lastErrorTs: most recent event_log timestamp with exit_code != 0
  //     (set by loadDashboard's fetch of /api/logging)
  //   - lastPluginsCount: length of the most recent /api/plugins response
  //     (kept up to date by loadPlugins() so the dashboard's Total
  //     Plugins stays correct without a dedicated /api/plugins call)
  //   - lastSinksCount: length of the most recent /api/sinks response
  //   - lastTargetsCount: length of the most recent /api/targets response
  //     (also refreshed by loadSinks()/loadAllTargets() on their views)
  //   - dashboardHealthInterval: 30s health-timer handle, cleared on nav away
  let lastErrorTs = null;
  let lastPluginsCount = null;
  let lastSinksCount = null;
  let lastTargetsCount = null;
  let dashboardHealthInterval = null;

  async function loadDashboard() {
    // On every entry, fire all data sources in parallel. Each
    // populates its own stat; the panel re-renders incrementally as each
    // resolves (the readme is static and renders with the page).
    $('stats-error').style.display = 'none';
    renderDashboardStats();

    const results = await Promise.allSettled([
      api('/plugins'),
      api('/subscriptions'),
      api('/subscriptions/activity?hours=24'),
      api('/health'),
      api('/sinks'),
      api('/targets'),
    ]);
    const [pluginsRes, subsRes, activityRes, healthRes, sinksRes, targetsRes] = results;
    const ok = (r) => r.status === 'fulfilled' ? r.value : null;
    const plugins = ok(pluginsRes);
    const subs = ok(subsRes);
    const activity = ok(activityRes) || {};
    const health = ok(healthRes);
    const sinks = ok(sinksRes);
    const targets = ok(targetsRes);

    // Caches used by renderDashboardStats (called on every SSE event).
    allSubsCache = subs || [];
    allSubsActivity = activity || {};

    // Last error: scan /logging for the most recent exit_code != 0.
    // Done in a separate .then so the main four stats can render first.
    api('/logging').then(logging => {
      if (Array.isArray(logging)) {
        const errEntry = logging.find(e => e.exit_code !== 0);
        if (errEntry) lastErrorTs = errEntry.executed_at;
        else lastErrorTs = null;
      }
      renderDashboardStats();
    }).catch(() => { /* leave lastErrorTs as-is */ });

    if (health) {
      renderDashboardHealth(health);
      startDashboardHealthTimer();
    } else {
      markHealthUnreachable();
    }
    // Cache the plugin count so the Total Plugins stat populates even
    // when the user has never visited the Data Sources page (which is
    // the only other path that updates lastPluginsCount).
    if (plugins) lastPluginsCount = plugins.length;
    // Same for sinks/targets: cache their counts and the target array so
    // the destination stats render without a dedicated view visit.
    if (sinks) lastSinksCount = sinks.length;
    if (Array.isArray(targets)) {
      lastTargetsCount = targets.length;
      targetsCache = targets;
    }
    renderDashboardStats();

    // If any of the four primary fetches failed, surface the inline
    // reload banner so the operator can recover without leaving the page.
    if (plugins === null || subs === null || activity === null) {
      $('stats-error').style.display = 'flex';
    }
  }

  function startDashboardHealthTimer() {
    stopDashboardHealthTimer();
    dashboardHealthInterval = setInterval(async () => {
      // Only keep polling while the dashboard is the visible view.
      // (navigate() stops the timer on any other view; this guard is
      // defense-in-depth in case the timer fires between view swaps.)
      if (currentView !== 'dashboard') {
        stopDashboardHealthTimer();
        return;
      }
      try {
        const h = await api('/health');
        renderDashboardHealth(h);
      } catch (e) {
        markHealthUnreachable();
      }
    }, 30000);
  }

  function stopDashboardHealthTimer() {
    if (dashboardHealthInterval !== null) {
      clearInterval(dashboardHealthInterval);
      dashboardHealthInterval = null;
    }
  }

  function renderDashboardStats() {
    // Each stat row reads from the in-memory caches populated by
    // loadDashboard() and by the SSE handlers. If the cache is empty
    // (first paint before the fetches complete), the row shows an em-dash.
    const subs = allSubsCache;
    const totalSubs = subs.length;
    $('stat-total-subs').textContent = totalSubs;

    // By type
    const sched = subs.filter(s => (s.sub_type || 'SCHEDULED') === 'SCHEDULED').length;
    const ev = subs.filter(s => s.sub_type === 'EVENT_BASED').length;
    $('stat-by-type').textContent = totalSubs === 0
      ? '0 scheduled · 0 event-based'
      : `${sched} scheduled · ${ev} event-based`;

    // By status — build a mini-bar with 4 segments:
    // ENABLED (green), IN_PROGRESS+ENQUEUED (amber/blue, shown as "Running"),
    // ERROR (red), DISABLED (gray). Width is proportional to count.
    const groups = {
      ENABLED:  { color: '#00C853', count: 0 },
      RUNNING:  { color: '#3D5AFE', count: 0 },
      ERROR:    { color: '#FF5252', count: 0 },
      DISABLED: { color: '#616161', count: 0 },
    };
    for (const s of subs) {
      if (s.status === 'ENABLED') groups.ENABLED.count++;
      else if (s.status === 'IN_PROGRESS' || s.status === 'ENQUEUED') groups.RUNNING.count++;
      else if (s.status === 'ERROR') groups.ERROR.count++;
      else if (s.status === 'DISABLED') groups.DISABLED.count++;
    }
    const bar = $('stat-status-bar');
    bar.innerHTML = '';
    if (totalSubs === 0) {
      const empty = document.createElement('div');
      empty.className = 'status-bar-empty';
      empty.textContent = 'no subscriptions yet';
      bar.appendChild(empty);
    } else {
      for (const [name, info] of Object.entries(groups)) {
        if (info.count === 0) continue;
        const pct = (info.count / totalSubs) * 100;
        const seg = document.createElement('div');
        seg.className = 'status-bar-seg';
        seg.style.width = pct + '%';
        seg.style.background = info.color;
        seg.title = `${name}: ${info.count}`;
        // Only overlay the count if the segment is wide enough to fit it.
        if (pct >= 10) seg.textContent = info.count;
        bar.appendChild(seg);
      }
    }

    // Last error
    $('stat-last-error').textContent = lastErrorTs ? relativeTime(lastErrorTs) : 'never';

    // 24h activity
    const total24h = Object.values(allSubsActivity).reduce((a, b) => a + b, 0);
    $('stat-activity').textContent = total24h;

    // Total plugins — populated by loadPlugins() (which runs every time
    // the Data Sources view is entered). On the very first paint of the
    // dashboard, this may still be null (no plugins count yet), in which
    // case the em-dash placeholder stays. Subsequent SSE-driven
    // re-renders use the cached value.
    if (typeof lastPluginsCount === 'number') {
      $('stat-total-plugins').textContent = lastPluginsCount;
    }

    // Total sinks & targets — populated by loadDashboard() and refreshed
    // on the Data Destinations / Targets views (see loadSinks / loadAllTargets).
    if (typeof lastSinksCount === 'number') {
      $('stat-total-sinks').textContent = lastSinksCount;
    }
    if (typeof lastTargetsCount === 'number') {
      $('stat-total-targets').textContent = lastTargetsCount;
    }

    // Target status — same mini-bar shape as the subscription status bar,
    // but derived from the targets' own statuses.
    const targetList = Array.isArray(targetsCache) ? targetsCache : [];
    const tGroups = {
      ENABLED:    { color: '#00C853', count: 0 },
      IN_PROGRESS: { color: '#3D5AFE', count: 0 },
      ERROR:      { color: '#FF5252', count: 0 },
      DISABLED:   { color: '#9E9E9E', count: 0 },
      DELETED:    { color: '#616161', count: 0 },
    };
    for (const t of targetList) {
      const st = t.status || 'ENABLED';
      if (tGroups[st]) tGroups[st].count++;
    }
    const tBar = $('stat-target-status-bar');
    tBar.innerHTML = '';
    if (targetList.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'status-bar-empty';
      empty.textContent = 'no targets yet';
      tBar.appendChild(empty);
    } else {
      for (const [name, info] of Object.entries(tGroups)) {
        if (info.count === 0) continue;
        const pct = (info.count / targetList.length) * 100;
        const seg = document.createElement('div');
        seg.className = 'status-bar-seg';
        seg.style.width = pct + '%';
        seg.style.background = info.color;
        seg.title = `${name}: ${info.count}`;
        if (pct >= 10) seg.textContent = info.count;
        tBar.appendChild(seg);
      }
    }

  }

  function renderDashboardHealth(h) {
    setHealthPill('db',       h.db === true);
    setHealthPill('redis',    h.redis === true);
    const regOk = h.registry_loaded === true;
    setHealthPill('registry', regOk);
    if (!regOk) {
      const pill = document.querySelector('.health-pill[data-sys="registry"]');
      if (pill) pill.title = 'Source registry is not loaded';
    }
    const sinkRegOk = h.sink_registry_loaded === true;
    setHealthPill('sink_registry', sinkRegOk);
    if (!sinkRegOk) {
      const pill = document.querySelector('.health-pill[data-sys="sink_registry"]');
      if (pill) pill.title = 'Destination registry is not loaded';
    }
  }

  function markHealthUnreachable() {
    setHealthPill('db', false);
    setHealthPill('redis', false);
    setHealthPill('registry', false);
    setHealthPill('sink_registry', false);
  }

  function setHealthPill(sys, ok) {
    const pill = document.querySelector(`.health-pill[data-sys="${sys}"]`);
    if (!pill) return;
    const dot = pill.querySelector('.health-dot');
    dot.classList.remove('health-ok', 'health-fail', 'health-pending');
    dot.classList.add(ok ? 'health-ok' : 'health-fail');
  }

  // Reload-link wiring (event delegation; survives re-renders).
  $('stats-reload').addEventListener('click', (ev) => {
    ev.preventDefault();
    loadDashboard();
  });
  // The Last-Error row is a clickable link to Recent Activity (errors view).
  document.querySelector('.stats-row[data-stat="last-error"]')
    ?.addEventListener('click', () => { location.hash = '#/activity'; });

  // ---- Subscriptions ----
  let currentPlugin = null;
  async function loadSubscriptions(pluginId) {
    try {
      const plugin = await api(`/plugins/${pluginId}`);
      currentPlugin = plugin;
      $('plugin-title').textContent = plugin.display_name || plugin.name;
      const iconEl = $('plugin-icon');
      iconEl.src = `/assets/${escapeHtml(plugin.icon)}`;
      iconEl.onerror = () => { iconEl.src = '/assets/default_icon.png'; };
      $('plugin-description').textContent = plugin.description || '';
      $('new-subscription-btn').onclick = () => openCreateForm(pluginId);
      $('edit-plugin-btn').onclick = () => {
        location.hash = `#/devlab?edit=${encodeURIComponent(pluginId)}`;
      };
      $('delete-plugin-btn').onclick = () => confirmDeletePlugin();
      const subs = await api(`/subscriptions?plugin_id=${encodeURIComponent(pluginId)}`);
      subscriptionsCache[pluginId] = subs;
      renderSubscriptions(subs, plugin);
      updateDeletePluginBtnState();
      // Measure after the title text + button widths have settled
      // (image icons, etc. can affect layout on first paint).
      requestAnimationFrame(syncBackButtonSpacing);
    } catch (e) { console.error(e); }
  }

  function relativeTime(iso) {
    if (!iso) return 'never';
    const t = new Date(iso).getTime();
    if (isNaN(t)) return 'never';
    const diff = Math.max(0, (Date.now() - t) / 1000);
    if (diff < 5) return 'just now';
    if (diff < 60) return `${Math.floor(diff)}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  }

  function renderSubscriptions(subs, plugin) {
    const list = $('subscription-list');
    list.innerHTML = '';
    if (!subs.length) {
      list.innerHTML = '<p class="sub-row-meta">No subscriptions yet. Create one to get started.</p>';
      return;
    }
    for (const sub of subs) {
      list.appendChild(buildSubscriptionRow(sub, plugin));
    }
  }

  function buildSubscriptionRow(sub, plugin) {
    const row = document.createElement('div');
    row.className = 'subscription-row';
    row.dataset.subId = sub.id;
    row.innerHTML = `
      <div class="sub-row-header">
        <div>
          <span class="sub-row-name">${escapeHtml(sub.name)}</span>
          <span class="badge badge-${sub.status}">${sub.status}</span>
          <span class="badge badge-${sub.access_level}">${sub.access_level}</span>
          <span class="sub-row-meta" style="margin-left: 8px;">24h activity: <span class="activity-count" data-sub="${sub.id}">...</span></span>
          <span class="sub-row-meta" style="margin-left: 8px;">Updated: <span class="last-updated" data-sub="${sub.id}">${escapeHtml(relativeTime(sub.last_updated))}</span></span>
          ${sub.last_message ? `<span class="sub-row-meta sub-row-message" style="margin-left: 8px;">Last Message: ${escapeHtml(sub.last_message)}</span>` : ''}
        </div>
        <div class="sub-row-actions"></div>
      </div>
      <div class="sub-row-meta">${escapeHtml(sub.description || '')}</div>
      <div class="sub-row-progress" data-sub="${sub.id}">
        ${sub.status === 'IN_PROGRESS' ? `
          <div class="progress-bar"><div class="progress-bar-fill" style="width: ${sub.progress || 0}%;"></div></div>
          <div class="sub-row-meta" style="margin-top: 4px;">${sub.progress || 0}%</div>
        ` : ''}
      </div>
    `;
    populateRowActions(row, sub, plugin);
    // Fetch activity count
    refreshActivityCount(sub.id);
    return row;
  }

  function populateRowActions(row, sub, plugin) {
    const actions = row.querySelector('.sub-row-actions');
    actions.innerHTML = '';
    const canAct = !['ERROR', 'DISABLED', 'DELETED'].includes(sub.status);
    if (canAct) {
      actions.appendChild(button('Update', 'btn-success', () => triggerSubscription(sub.id)));
    }
    const edit = button('Edit', 'btn-primary', () => openEditForm(sub.id, plugin.plugin_id));
    edit.disabled = sub.status === 'DELETED';
    actions.appendChild(edit);
    if (sub.status === 'ERROR' || sub.status === 'DISABLED') {
      actions.appendChild(button('Enable', 'btn-primary', (e) => setStatus(sub.id, 'ENABLED', e.target)));
    } else if (sub.status !== 'DELETED') {
      actions.appendChild(button('Disable', 'btn-destructive', (e) => setStatus(sub.id, 'DISABLED', e.target)));
    }
    if (sub.status !== 'DELETED') {
      actions.appendChild(button('Delete', 'btn-destructive', () => confirmDelete(sub.id, sub.name)));
    }
  }

  function updateSubscriptionRow(sub, plugin) {
    const list = $('subscription-list');
    let row = list.querySelector(`.subscription-row[data-sub-id="${sub.id}"]`);
    if (!row) {
      // New subscription (e.g., just created in another tab) — append.
      // Remove the "no subscriptions" placeholder first if present.
      const placeholder = list.querySelector('.sub-row-meta');
      if (placeholder && placeholder.textContent.includes('No subscriptions')) {
        placeholder.remove();
      }
      row = buildSubscriptionRow(sub, plugin);
      list.appendChild(row);
      return;
    }
    // Update the visible status badge in place.
    const statusBadge = row.querySelector(`.badge-${sub.status}`);
    if (!statusBadge) {
      // Status changed; re-render the row.
      const fresh = buildSubscriptionRow(sub, plugin);
      row.replaceWith(fresh);
      return;
    }
    // Access level may have changed via Edit (status didn't). If the
    // current access-level badge doesn't match, re-render so the badge
    // updates in place.
    const accessBadge = row.querySelector(`.badge-${sub.access_level}`);
    if (!accessBadge) {
      const fresh = buildSubscriptionRow(sub, plugin);
      row.replaceWith(fresh);
      return;
    }
    // Update last-updated
    const lu = row.querySelector(`.last-updated[data-sub="${sub.id}"]`);
    if (lu) lu.textContent = relativeTime(sub.last_updated);
    // Update last-message
    let msgEl = row.querySelector(`.sub-row-message`);
    if (sub.last_message) {
      if (msgEl) {
        msgEl.textContent = `Last Message: ${sub.last_message}`;
      } else {
        const lu2 = row.querySelector(`.last-updated[data-sub="${sub.id}"]`);
        if (lu2 && lu2.parentElement) {
          msgEl = document.createElement('span');
          msgEl.className = 'sub-row-meta sub-row-message';
          msgEl.style.marginLeft = '8px';
          msgEl.textContent = `Last Message: ${sub.last_message}`;
          lu2.parentElement.appendChild(msgEl);
        }
      }
    } else if (msgEl) {
      msgEl.remove();
    }
    // Update progress block
    const progressWrap = row.querySelector(`.sub-row-progress[data-sub="${sub.id}"]`);
    if (progressWrap) {
      if (sub.status === 'IN_PROGRESS') {
        progressWrap.innerHTML = `
          <div class="progress-bar"><div class="progress-bar-fill" style="width: ${sub.progress || 0}%;"></div></div>
          <div class="sub-row-meta" style="margin-top: 4px;">${sub.progress || 0}%</div>
        `;
      } else {
        progressWrap.innerHTML = '';
      }
    }
    // Re-populate actions (status changed → different buttons)
    populateRowActions(row, sub, plugin);
    // Re-fetch activity count (spec: refreshed on every subscription_update)
    refreshActivityCount(sub.id);
  }

  function removeSubscriptionRow(subId) {
    const list = $('subscription-list');
    const row = list.querySelector(`.subscription-row[data-sub-id="${subId}"]`);
    if (!row) return;
    row.remove();
    // If the list is now empty, show the placeholder.
    if (!list.querySelector('.subscription-row')) {
      const placeholder = document.createElement('p');
      placeholder.className = 'sub-row-meta';
      placeholder.textContent = 'No subscriptions yet. Create one to get started.';
      list.appendChild(placeholder);
    }
  }

  function refreshActivityCount(subId) {
    api(`/subscriptions/${subId}/activity`).then(a => {
      const el = $('subscription-list').querySelector(`.activity-count[data-sub="${subId}"]`);
      if (el) el.textContent = a.count;
    }).catch(() => {});
  }

  function button(label, cls, handler) {
    const b = document.createElement('button');
    b.className = `btn ${cls}`;
    b.textContent = label;
    b.addEventListener('click', handler);
    return b;
  }

  async function triggerSubscription(subId) {
    try { await api(`/subscriptions/${subId}/trigger`, { method: 'POST' }); }
    catch (e) { alert('Trigger failed: ' + e.message); }
  }

  async function setStatus(subId, status, btn) {
    if (btn) btn.disabled = true;
    try { await api(`/subscriptions/${subId}/status`, { method: 'PUT', body: JSON.stringify({ status }) }); }
    catch (e) { alert('Status change failed: ' + e.message); if (btn) btn.disabled = false; }
  }

  async function confirmDelete(subId, name) {
    if (!confirm(`Are you sure you want to delete subscription '${name}'?`)) return;
    try { await api(`/subscriptions/${subId}`, { method: 'DELETE' }); }
    catch (e) { alert('Delete failed: ' + e.message); }
  }

  function updateDeletePluginBtnState() {
    const btn = $('delete-plugin-btn');
    if (!btn) return;
    if (!currentPlugin) {
      btn.disabled = true;
      btn.title = '';
      return;
    }
    const subs = subscriptionsCache[currentPlugin.plugin_id] || [];
    const empty = subs.length === 0;
    btn.disabled = !empty;
    btn.title = empty
      ? 'Delete this source and its file from disk'
      : 'Cannot delete a source with existing subscriptions. Delete them first.';
  }

  async function confirmDeletePlugin() {
    if (!currentPlugin) return;
    const name = currentPlugin.name;
    if (!confirm(`Are you sure you want to delete the source '${name}'?\n\nThe source file will be removed from disk. This action cannot be undone.`)) return;
    try {
      await api(`/plugins/${encodeURIComponent(name)}`, { method: 'DELETE' });
      currentPlugin = null;
      subscriptionsCache = {};
      location.hash = '#/';
      await loadPlugins();
    } catch (e) { alert('Delete source failed: ' + e.message); }
  }

  // ---- Form ----
  let formPluginId = null;
  let formSubId = null;
  let formSchema = null;
  let formPasswordFields = [];

  async function openCreateForm(pluginId) {
    formPluginId = pluginId;
    formSubId = null;
    const data = await api(`/plugins/${pluginId}/schema`);
    formSchema = data.schema;
    formPasswordFields = data.password_fields || [];
    const plugin = await api(`/plugins/${pluginId}`);
    $('form-title').textContent = `Create Subscription (${plugin.name})`;
    await buildForm(plugin, null);
    $('form-modal').style.display = 'flex';
  }

  async function openEditForm(subId, pluginId) {
    formPluginId = pluginId;
    formSubId = subId;
    const data = await api(`/plugins/${pluginId}/schema`);
    formSchema = data.schema;
    formPasswordFields = data.password_fields || [];
    const sub = await api(`/subscriptions/${subId}`);
    const plugin = await api(`/plugins/${pluginId}`);
    $('form-title').textContent = `Edit Subscription (${plugin.name})`;
    await buildForm(plugin, sub);
    $('form-modal').style.display = 'flex';
  }

  function humanizeKey(key) {
    return key.split('_').filter(Boolean)
      .map(w => w.charAt(0).toUpperCase() + w.slice(1))
      .join(' ');
  }

  async function buildForm(plugin, sub) {
    const fields = $('form-fields');
    fields.innerHTML = '';
    const isEdit = !!sub;
    // Name (only on create) — use sub_name to avoid colliding with a schema
    // field that may also be called "name" (e.g. longRunningSuccessPlugin's
    // config has a required "name" property). FormData silently overwrites
    // duplicate names, so without the prefix the schema field would clobber
    // the subscription name and the form would 400 on submit.
    if (!isEdit) {
      const div = document.createElement('div');
      div.className = 'form-field';
      div.innerHTML = `
        <label>Name <span class="form-field-error">*</span></label>
        <input type="text" name="sub_name" required maxlength="${SUBSCRIPTION_NAME_MAX_LEN}" />
        <small class="form-field-error" id="subscription-name-error" style="display:none"></small>
      `;
      fields.appendChild(div);
      const nameInput = div.querySelector('input[name="sub_name"]');
      const nameErr = div.querySelector('#subscription-name-error');
      const updateSubscriptionNameValidation = () => {
        const err = validateSubscriptionName(nameInput.value);
        if (err) {
          nameErr.textContent = err;
          nameErr.style.display = 'block';
          nameInput.classList.add('invalid');
        } else {
          nameErr.style.display = 'none';
          nameInput.classList.remove('invalid');
        }
      };
      nameInput.addEventListener('input', updateSubscriptionNameValidation);
    }
    // Cron
    const cronDiv = document.createElement('div');
    cronDiv.className = 'form-field';
    cronDiv.innerHTML = `
      <label>Cron Expression</label>
      <input type="text" name="cron" value="${escapeHtml(sub?.cron || (plugin.sub_type === 'SCHEDULED' ? '0 0 * * 0' : '0 0 * * *'))}" />
      <small class="sub-row-meta">Default: ${plugin.sub_type === 'SCHEDULED' ? '0 0 * * 0' : '0 0 * * *'}</small>
    `;
    fields.appendChild(cronDiv);
    // Access level
    const accDiv = document.createElement('div');
    accDiv.className = 'form-field';
    const currentAcc = sub?.access_level || plugin.default_access_level;
    accDiv.innerHTML = `
      <label>Access Level</label>
      <select name="access_level">
        <option value="PRIVATE" ${currentAcc === 'PRIVATE' ? 'selected' : ''}>PRIVATE</option>
        <option value="PUBLIC" ${currentAcc === 'PUBLIC' ? 'selected' : ''}>PUBLIC</option>
      </select>
    `;
    fields.appendChild(accDiv);
    // Webhook trigger URL (edit mode only)
    if (isEdit) {
      const div = document.createElement('div');
      div.className = 'form-field';
      const url = `${window.location.origin}/api/subscriptions/${sub.id}/trigger`;
      const wrapper = document.createElement('div');
      wrapper.style.cssText = 'position:relative;display:flex;align-items:center;';
      const input = document.createElement('input');
      input.type = 'text';
      input.readOnly = true;
      input.value = url;
      input.style.cssText = 'width:100%;padding:6px 32px 6px 8px;font-size:12px;background:#1A1A1A;border:1px solid #333;border-radius:4px;color:#ccc;box-sizing:border-box;';
      const icon = document.createElement('span');
      icon.textContent = '📋';
      icon.title = 'Copy';
      icon.style.cssText = 'position:absolute;right:6px;top:50%;transform:translateY(-50%);cursor:pointer;font-size:14px;line-height:1;';
      icon.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(url);
        } catch (e) {
          const ta = document.createElement('textarea');
          ta.value = url;
          ta.style.position = 'fixed'; ta.style.opacity = '0';
          document.body.appendChild(ta);
          ta.select();
          document.execCommand('copy');
          document.body.removeChild(ta);
        }
        icon.textContent = '✓';
        setTimeout(() => { icon.textContent = '📋'; }, 1500);
      });
      wrapper.appendChild(input);
      wrapper.appendChild(icon);
      const label = document.createElement('label');
      label.textContent = 'Webhook URL';
      div.appendChild(label);
      div.appendChild(wrapper);
      const hint = document.createElement('small');
      hint.className = 'sub-row-meta';
      hint.innerHTML = 'POST to this URL with <code>Authorization: Bearer &lt;webhook_key&gt;</code> to trigger an immediate run.';
      div.appendChild(hint);
      fields.appendChild(div);
    }
    // Schema fields
    const props = formSchema.properties || {};
    const required = formSchema.required || [];
    for (const [key, spec] of Object.entries(props)) {
      if (key === '_extra_param_1' || key === '_extra_param_2' || key === '_extra_param_3') continue;
      const div = document.createElement('div');
      div.className = 'form-field';
      const isRequired = required.includes(key);
      const isPassword = spec.format === 'password';
      const value = sub?.config?.[key];
      const resolvedValue = isPassword ? '' : (value !== undefined ? value : spec.default);
      let input = '';
      // Dynamic enum: fetch options from a custom route if x-enum-source is set
      if (spec['x-enum-source'] && !spec.enum) {
        const srcUrl = spec['x-enum-source'];
        try {
          let data = enumSourceCache.get(srcUrl);
          if (!data) {
            const resp = await fetch(srcUrl);
            if (resp.ok) {
              data = await resp.json();
              enumSourceCache.set(srcUrl, data);
            }
          }
          if (data) {
            spec.enum = data.versions || [];
            spec.enumLabels = data.labels || {};
            spec.enumGroups = data.groups || undefined;
          }
        } catch (e) { /* fall through to text input */ }
      }
      if (spec.enum) {
        const labels = spec.enumLabels || {};
        const groups = spec.enumGroups;
        let optionsHtml = '';
        if (groups) {
          for (const [groupName, values] of Object.entries(groups)) {
            const opts = values.map(o =>
              `<option value="${o}" ${(value !== undefined ? value : spec.default) === o ? 'selected' : ''}>${labels[o] || o}</option>`
            ).join('');
            optionsHtml += `<optgroup label="${groupName}">${opts}</optgroup>`;
          }
        } else {
          optionsHtml = spec.enum.map(o =>
            `<option value="${o}" ${(value !== undefined ? value : spec.default) === o ? 'selected' : ''}>${labels[o] || o}</option>`
          ).join('');
        }
        if (groups) {
          input = `<input type="text" class="enum-search" placeholder="Type to filter..." oninput="filterEnumSelect(this, '${key}')" />` +
            `<select name="${key}">${optionsHtml}</select>`;
        } else {
          input = `<select name="${key}">${optionsHtml}</select>`;
        }
      } else if (spec.type === 'boolean') {
        input = `<input type="checkbox" name="${key}" ${resolvedValue ? 'checked' : ''} />`;
      } else if (spec.type === 'integer') {
        input = `<input type="number" step="1" name="${key}" value="${resolvedValue != null ? escapeHtml(String(resolvedValue)) : ''}" />`;
      } else if (spec.type === 'number') {
        input = `<input type="number" name="${key}" value="${resolvedValue != null ? escapeHtml(String(resolvedValue)) : ''}" />`;
      } else if (spec.format === 'textarea') {
        input = `<textarea name="${key}" rows="6" style="width:100%;font-family:monospace;font-size:12px">${escapeHtml(resolvedValue ?? '')}</textarea>`;
      } else {
        const inputType = isPassword ? 'password' : 'text';
        input = `<input type="${inputType}" name="${key}" value="${escapeHtml(resolvedValue ?? '')}" ${isPassword ? 'autocomplete="new-password"' : ''} />`;
      }
      div.innerHTML = `
        <label>${humanizeKey(key)}${isRequired ? ' *' : ''} ${isPassword ? '<small>(password)</small>' : ''}</label>
        ${input}
      `;
      fields.appendChild(div);
    }
    // Extra params
    const extraDiv = document.createElement('div');
    extraDiv.innerHTML = '<label style="color:#9E9E9E;">Extra Parameters</label>';
    for (let i = 1; i <= 3; i++) {
      const key = `_extra_param_${i}`;
      const div = document.createElement('div');
      div.className = 'form-field';
      div.innerHTML = `
        <label>${humanizeKey(key)}</label>
        <input type="text" name="${key}" value="${escapeHtml(sub?.config?.[key] || '')}" />
      `;
      extraDiv.appendChild(div);
    }
    fields.appendChild(extraDiv);
  }

  $('form-cancel').addEventListener('click', () => { $('form-modal').style.display = 'none'; });

  function parseFormValue(key, value) {
    const spec = (formSchema.properties || {})[key];
    const isRequired = (formSchema.required || []).includes(key);
    if (value === '' && !isRequired) return undefined;
    if (spec?.type === 'integer') { const n = parseInt(value, 10); return isNaN(n) ? undefined : n; }
    if (spec?.type === 'number') { const n = parseFloat(value); return isNaN(n) ? undefined : n; }
    return value;
  }

  $('sub-form').addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const fd = new FormData($('sub-form'));
    const config = {};
    let name = null;
    let cron = null;
    let access_level = null;
    if (formSubId) {
      // Edit: every form field is part of the config. The "name" field on
      // this form is the *config* name (schema-defined), not the
      // subscription's display name — they're different concepts.
      for (const [k, v] of fd.entries()) {
        if (k === 'cron') { cron = v; continue; }
        if (k === 'access_level') { access_level = v; continue; }
        const parsed = parseFormValue(k, v);
        if (parsed !== undefined) config[k] = parsed;
      }
      // Checkboxes (only present in config, not at the top level)
      for (const cb of document.querySelectorAll('#form-fields input[type="checkbox"]')) {
        config[cb.name] = cb.checked;
      }
    } else {
      // Create: the subscription's display name is in the "sub_name" input
      // (renamed to avoid colliding with a schema field called "name").
      for (const [k, v] of fd.entries()) {
        if (k === 'sub_name') { name = v; continue; }
        if (k === 'cron') { cron = v; continue; }
        if (k === 'access_level') { access_level = v; continue; }
        const parsed = parseFormValue(k, v);
        if (parsed !== undefined) config[k] = parsed;
      }
      // Checkboxes
      for (const cb of document.querySelectorAll('#form-fields input[type="checkbox"]')) {
        config[cb.name] = cb.checked;
      }
    }
    if (formSubId) {
      // Edit
      try {
        const body = { config, cron, access_level };
        await api(`/subscriptions/${formSubId}`, { method: 'PUT', body: JSON.stringify(body) });
        $('form-modal').style.display = 'none';
      } catch (e) { alert('Save failed: ' + e.message); }
    } else {
      // Create
      const nameErr = validateSubscriptionName(name);
      if (nameErr) {
        const errEl = $('subscription-name-error');
        if (errEl) { errEl.textContent = nameErr; errEl.style.display = 'block'; }
        const nameInput = $('sub-form').querySelector('input[name="sub_name"]');
        if (nameInput) {
          nameInput.classList.add('invalid');
          nameInput.focus();
        }
        return;
      }
      try {
        const body = { name, config, cron, access_level };
        await api(`/subscriptions/${formPluginId}`, { method: 'POST', body: JSON.stringify(body) });
        $('form-modal').style.display = 'none';
        if (currentPluginId) loadSubscriptions(currentPluginId);
      } catch (e) { alert('Save failed: ' + e.message); }
    }
  });

  // ---- Activity ----
  let activityCache = [];
  let activitySort = { key: 'executed_at', dir: 'desc' };
  const EXIT_LABELS = { 0: 'Success', 1: 'Error', 2: 'Timeout', 3: 'Config Error' };

  function sortActivity(rows, key, dir) {
    const sorted = rows.slice();
    sorted.sort((a, b) => {
      let av = a[key], bv = b[key];
      if (key === 'executed_at') {
        av = new Date(av).getTime();
        bv = new Date(bv).getTime();
      } else if (key === 'exit_code') {
        // Map exit codes to severity for sort: 0 < 3 < 1, 2
        const order = { 0: 0, 3: 1, 1: 2, 2: 3 };
        av = order[av] ?? 99;
        bv = order[bv] ?? 99;
      } else {
        av = String(av || '');
        bv = String(bv || '');
      }
      if (av < bv) return dir === 'asc' ? -1 : 1;
      if (av > bv) return dir === 'asc' ? 1 : -1;
      return 0;
    });
    return sorted;
  }

  function renderActivity() {
    const list = $('activity-list');
    list.innerHTML = '';
    const rows = sortActivity(activityCache, activitySort.key, activitySort.dir);
    // Update sort indicators
    document.querySelectorAll('.activity-header .sortable').forEach(el => {
      el.classList.remove('sorted-asc', 'sorted-desc');
      if (el.dataset.sort === activitySort.key) {
        el.classList.add(activitySort.dir === 'asc' ? 'sorted-asc' : 'sorted-desc');
      }
    });
    for (const r of rows) {
      const row = document.createElement('div');
      row.className = 'activity-row';
      const label = EXIT_LABELS[r.exit_code] || 'Unknown';
      row.innerHTML = `
        <span class="col-view"><button class="btn btn-primary" data-event-id="${escapeHtml(r.id)}">View</button></span>
        <span class="col-plugin">${escapeHtml(r.plugin_display_name || r.plugin_id || '')}</span>
        <span class="col-name">${escapeHtml(r.subscription_name || r.subscription_id)}</span>
        <span class="col-time">${escapeHtml(new Date(r.executed_at).toLocaleString())}</span>
        <span class="col-status"><span class="badge badge-${r.exit_code}">${escapeHtml(label)}</span></span>
      `;
      list.appendChild(row);
    }
    // Wire up View buttons
    list.querySelectorAll('button[data-event-id]').forEach(btn => {
      btn.addEventListener('click', () => openEventDetail(btn.dataset.eventId));
    });
  }

  async function loadActivity() {
    try {
      activityCache = await api('/logging');
      renderActivity();
    } catch (e) { console.error(e); }
  }

  // Sortable column header clicks
  document.querySelectorAll('.activity-header .sortable').forEach(el => {
    el.addEventListener('click', () => {
      const key = el.dataset.sort;
      if (activitySort.key === key) {
        activitySort.dir = activitySort.dir === 'asc' ? 'desc' : 'asc';
      } else {
        activitySort.key = key;
        activitySort.dir = (key === 'executed_at' || key === 'exit_code') ? 'desc' : 'asc';
      }
      renderActivity();
    });
  });

  $('clear-log-btn').addEventListener('click', async () => {
    if (!confirm('This will permanently delete all execution history. This action cannot be undone.')) return;
    try { await api('/logging', { method: 'DELETE' }); loadActivity(); }
    catch (e) { alert('Clear failed: ' + e.message); }
  });

  // ---- Event detail (full-screen view) ----
  function openEventDetail(eventId) {
    const ev = activityCache.find(r => r.id === eventId);
    if (!ev) return;
    const body = $('event-detail-body');
    const label = EXIT_LABELS[ev.exit_code] || 'Unknown';
    body.innerHTML = `
      <div class="detail-row">
        <span class="detail-label">Plugin</span>
        <span class="detail-value">${escapeHtml(ev.plugin_display_name || ev.plugin_id || '')}</span>
      </div>
      <div class="detail-row">
        <span class="detail-label">Subscription</span>
        <span class="detail-value">${escapeHtml(ev.subscription_name || ev.subscription_id)}</span>
      </div>
      <div class="detail-row">
        <span class="detail-label">Timestamp</span>
        <span class="detail-value">${escapeHtml(new Date(ev.executed_at).toLocaleString())}</span>
      </div>
      <div class="detail-row">
        <span class="detail-label">Status</span>
        <span class="detail-value"><span class="badge badge-${ev.exit_code}">${escapeHtml(label)}</span> (exit code ${ev.exit_code})</span>
      </div>
      <div class="detail-row">
        <span class="detail-label">Error / Output</span>
        <pre class="detail-exit">${escapeHtml(ev.exit_string || '(empty)')}</pre>
      </div>
    `;
    showOnly('view-event-detail');
    currentView = 'event-detail';
  }

  $('event-detail-back').addEventListener('click', () => {
    showOnly('view-activity');
    currentView = 'activity';
  });

  // ---- All Subscriptions (cross-plugin list) ----
  let allSubsCache = [];
  let allSubsActivity = {};
  let sinksCache = {};
  let targetsCache = {};
  let expandedTargets = new Set();
  let allSubsSort = { key: 'last_updated', dir: 'desc' };

  async function loadAllSubscriptions() {
    try {
      const [subs, activity] = await Promise.all([
        api('/subscriptions'),
        api('/subscriptions/activity'),
      ]);
      allSubsCache = subs;
      allSubsActivity = activity || {};
      renderAllSubscriptionsTable();
    } catch (e) { console.error(e); }
  }

  function sortAllSubs(rows, key, dir) {
    const sorted = rows.slice();
    sorted.sort((a, b) => {
      let av = a[key], bv = b[key];
      if (key === 'last_updated') {
        av = av ? new Date(av).getTime() : 0;
        bv = bv ? new Date(bv).getTime() : 0;
      } else if (key === 'activity_24h') {
        av = allSubsActivity[a.id] || 0;
        bv = allSubsActivity[b.id] || 0;
      } else {
        av = String(av || '');
        bv = String(bv || '');
      }
      if (av < bv) return dir === 'asc' ? -1 : 1;
      if (av > bv) return dir === 'asc' ? 1 : -1;
      return 0;
    });
    return sorted;
  }

  function renderAllSubscriptionsTable() {
    const list = $('all-subs-list');
    list.innerHTML = '';
    document.querySelectorAll('.all-subs-header .sortable').forEach(el => {
      el.classList.remove('sorted-asc', 'sorted-desc');
      if (el.dataset.sort === allSubsSort.key) {
        el.classList.add(allSubsSort.dir === 'asc' ? 'sorted-asc' : 'sorted-desc');
      }
    });
    const rows = sortAllSubs(allSubsCache, allSubsSort.key, allSubsSort.dir);
    if (!rows.length) {
      list.innerHTML = '<p class="sub-row-meta" style="padding: 16px;">No subscriptions yet. Create one from a Data Source.</p>';
      return;
    }
    for (const sub of rows) {
      list.appendChild(buildAllSubsRow(sub));
    }
  }

  function buildAllSubsRow(sub) {
    const row = document.createElement('div');
    row.className = 'all-subs-row';
    const status = sub.status || 'ENABLED';
    const access = sub.access_level || 'PRIVATE';
    const count = allSubsActivity[sub.id] != null ? allSubsActivity[sub.id] : '…';
    const isTerminal = ['DELETED'].includes(status);
    const canTrigger = ['ENABLED', 'ENQUEUED', 'IN_PROGRESS'].includes(status);
    const canEnable = status === 'ERROR' || status === 'DISABLED';
    const toggleLabel = canEnable ? 'Enable' : 'Disable';
    const toggleClass = canEnable ? 'btn-primary' : 'btn-destructive';
    row.innerHTML = `
      <span class="all-subs-col-name">${escapeHtml(sub.name)}</span>
      <span class="all-subs-col-plugin">${escapeHtml(sub.plugin_display_name || sub.plugin_id)}</span>
      <span class="all-subs-col-status"><span class="badge badge-${status}">${escapeHtml(status)}${status === 'IN_PROGRESS' && Number.isFinite(sub.progress) ? ` (${Math.round(sub.progress)}%)` : ''}</span></span>
      <span class="all-subs-col-access"><span class="badge badge-${access}">${escapeHtml(access)}</span></span>
      <span class="all-subs-col-activity">${count}</span>
      <span class="all-subs-col-updated">${escapeHtml(relativeTime(sub.last_updated))}</span>
      <span class="all-subs-col-edit"><button class="btn btn-primary" data-act="edit" data-sub="${escapeHtml(sub.id)}" data-plugin="${escapeHtml(sub.plugin_id)}" ${isTerminal ? 'disabled' : ''}>Edit</button></span>
      <span class="all-subs-col-toggle"><button class="btn ${toggleClass}" data-act="toggle" data-sub="${escapeHtml(sub.id)}" data-target="${canEnable ? 'ENABLED' : 'DISABLED'}" ${isTerminal ? 'disabled' : ''}>${toggleLabel}</button></span>
      <span class="all-subs-col-update"><button class="btn btn-success" data-act="update" data-sub="${escapeHtml(sub.id)}" data-name="${escapeHtml(sub.name)}" ${canTrigger ? '' : 'disabled'}>Update</button></span>
    `;
    row.querySelectorAll('button[data-act]').forEach(btn => {
      btn.addEventListener('click', () => handleAllSubsAction(btn.dataset));
    });
    return row;
  }

  async function handleAllSubsAction(ds) {
    if (ds.act === 'edit') {
      try { await openEditForm(ds.sub, ds.plugin); }
      catch (e) { alert('Edit failed: ' + e.message); }
    } else if (ds.act === 'toggle') {
      const btn = document.querySelector(`button[data-act="toggle"][data-sub="${ds.sub}"]`);
      if (btn) btn.disabled = true;
      try { await api(`/subscriptions/${ds.sub}/status`, { method: 'PUT', body: JSON.stringify({ status: ds.target }) }); }
      catch (e) { alert('Status change failed: ' + e.message); if (btn) btn.disabled = false; }
    } else if (ds.act === 'update') {
      try { await api(`/subscriptions/${ds.sub}/trigger`, { method: 'POST' }); }
      catch (e) { alert('Update failed: ' + e.message); }
    }
  }

  // Sortable column header clicks for all-subs table
  document.querySelectorAll('.all-subs-header .sortable').forEach(el => {
    el.addEventListener('click', () => {
      const key = el.dataset.sort;
      if (allSubsSort.key === key) {
        allSubsSort.dir = allSubsSort.dir === 'asc' ? 'desc' : 'asc';
      } else {
        allSubsSort.key = key;
        allSubsSort.dir = (key === 'last_updated' || key === 'activity_24h') ? 'desc' : 'asc';
      }
      renderAllSubscriptionsTable();
    });
  });

  // ---- Destinations (Sinks) ----
  async function loadSinks() {
    try {
      const services = await api('/sinks');
      sinksCache = services;
      lastSinksCount = services.length;
      const grid = $('destination-grid');
      grid.innerHTML = '';
      for (const s of services) {
        const card = document.createElement('div');
        card.className = 'plugin-card';
        card.innerHTML = `
          <img src="/assets/${escapeHtml(s.icon)}" onerror="this.src='/assets/default_icon.png'" alt="" />
          <div>
            <div class="plugin-name">${escapeHtml(s.display_name || s.name)}</div>
          </div>
        `;
        card.addEventListener('click', () => { location.hash = `#/destinations-detail/${s.service_id}`; });
        grid.appendChild(card);
      }
    } catch (e) { console.error(e); }
  }

  // ---- Targets (per Destination) ----
  async function loadTargets(serviceId) {
    try {
      const services = await api('/sinks');
      const svc = services.find(s => s.service_id === serviceId);
      currentSink = svc || null;
      $('destination-description').textContent = (svc && svc.description) || '';
      if (svc) {
        $('destination-title').textContent = svc.display_name || svc.name;
        const iconEl = $('destination-icon');
        iconEl.src = `/assets/${escapeHtml(svc.icon)}`;
        iconEl.onerror = () => { iconEl.src = '/assets/default_icon.png'; };
      }
      const targets = await api(`/sinks/${serviceId}/targets`);
      renderTargets(targets);
      $('create-target-btn').onclick = () => openTargetForm(serviceId, null);
      $('edit-destination-btn').onclick = () => {
        if (currentSink && currentSink.name) {
          location.hash = `#/devlab/destination?edit=${encodeURIComponent(currentSink.name)}`;
        }
      };
      $('delete-destination-btn').onclick = () => confirmDeleteSink();
      updateDeleteSinkBtnState(targets);
    } catch (e) { console.error(e); }
  }

  function updateDeleteSinkBtnState(targetList) {
    const btn = $('delete-destination-btn');
    if (!btn) return;
    const list = targetList || [];
    const empty = list.length === 0;
    btn.disabled = !empty;
    btn.title = empty
      ? 'Delete this Destination and its file from disk'
      : 'Cannot delete a Destination with attached targets. Delete them first.';
  }

  async function confirmDeleteSink() {
    if (!currentSink) return;
    const name = currentSink.name;
    if (!confirm(`Are you sure you want to delete the Destination '${name}'?\n\nThe service file will be removed from disk. This action cannot be undone.`)) return;
    try {
      await api(`/sinks/${currentSink.service_id}`, { method: 'DELETE' });
      currentSink = null;
      location.hash = '#/data-destinations';
      await loadSinks();
    } catch (e) { alert('Delete Destination failed: ' + e.message); }
  }

  function renderTargets(targets) {
    const list = $('target-list');
    list.innerHTML = '';
    if (!targets || !targets.length) {
      list.innerHTML = '<p class="sub-row-meta">No targets yet. Create one to get started.</p>';
      return;
    }
    for (const t of targets) {
      list.appendChild(buildTargetRow(t));
    }
  }

  function buildTargetRow(ds) {
    const row = document.createElement('div');
    row.className = 'subscription-row';
    row.dataset.dsId = ds.target_id;
    const status = ds.status || 'ENABLED';
    const isTerminal = status === 'DELETED';
    const canUpdate = ['ENABLED', 'ENQUEUED', 'IN_PROGRESS'].includes(status);
    const canEnable = status === 'ERROR' || status === 'DISABLED';
    const toggleLabel = canEnable ? 'Enable' : 'Disable';
    const toggleClass = canEnable ? 'btn-primary' : 'btn-destructive';
    const isExpanded = expandedTargets.has(ds.target_id);
    const subCount = (ds.subscriptions && ds.subscriptions.length) || 0;
    row.innerHTML = `
      <div class="sub-row-header">
        <div>
          <span class="sub-row-chevron" data-chevron>${isExpanded ? '▾' : '▸'}</span>
          <span class="sub-row-name">${escapeHtml(ds.name)}</span>
          <span class="badge badge-${status}">${status}</span>
          <span class="sub-row-meta" style="margin-left:8px;">${subCount} sub${subCount === 1 ? '' : 's'}</span>
          <span class="sub-row-meta" style="margin-left:8px;">Updated: ${escapeHtml(relativeTime(ds.last_updated))}</span>
        </div>
        <div class="sub-row-actions"></div>
      </div>
      <div class="target-children" style="display:${isExpanded ? 'block' : 'none'};"></div>
    `;
    const chevron = row.querySelector('[data-chevron]');
    const children = row.querySelector('.target-children');
    if (subCount > 0) {
      chevron.addEventListener('click', () => {
        const open = children.style.display !== 'none';
        children.style.display = open ? 'none' : 'block';
        chevron.textContent = open ? '▸' : '▾';
        if (open) expandedTargets.delete(ds.target_id);
        else expandedTargets.add(ds.target_id);
      });
    } else {
      chevron.style.visibility = 'hidden';
    }
    const actions = row.querySelector('.sub-row-actions');
    if (canUpdate) {
      actions.appendChild(button('Update', 'btn-success', () => triggerTargetUpdate(ds.target_id)));
    }
    actions.appendChild(button('Edit', 'btn-primary', () => openTargetForm(ds.service_id, ds)));
    if (canEnable) {
      actions.appendChild(button('Enable', 'btn-primary', () => setTargetStatus(ds.target_id, 'ENABLED')));
    } else if (!isTerminal) {
      actions.appendChild(button('Disable', 'btn-destructive', () => setTargetStatus(ds.target_id, 'DISABLED')));
    }
    if (!isTerminal) {
      actions.appendChild(button('Delete', 'btn-destructive', () => confirmDeleteTarget(ds)));
    }
    buildTargetChildren(children, ds);
    return row;
  }

  function buildTargetChildren(container, ds) {
    container.innerHTML = '';
    const subs = ds.subscriptions || [];
    if (!subs.length) {
      container.innerHTML = '<p class="sub-row-meta" style="padding:8px 0 8px 24px;">No linked subscriptions.</p>';
      return;
    }
    for (const s of subs) {
      const child = document.createElement('div');
      child.className = 'target-child-row';
      const st = s.status || 'ENABLED';
      const isTerminal = st === 'DELETED';
      const canEnable = st === 'ERROR' || st === 'DISABLED';
      const toggleLabel = canEnable ? 'Enable' : 'Disable';
      const toggleClass = canEnable ? 'btn-primary' : 'btn-destructive';
      const meta = [s.last_updated ? `Updated: ${relativeTime(s.last_updated)}` : null,
                    s.last_message ? escapeHtml(s.last_message) : null]
        .filter(Boolean).join(' · ');
      child.innerHTML = `
        <span class="sub-row-chevron" style="visibility:hidden;">▸</span>
        <span class="sub-row-name">${escapeHtml(s.subscription_name || s.subscription_id)}</span>
        <span class="badge badge-${st}">${st}</span>
        <span class="sub-row-meta" style="margin-left:8px;">${meta}</span>
      `;
      const actions = document.createElement('span');
      actions.className = 'target-child-actions';
      if (!isTerminal) {
        actions.appendChild(button(toggleLabel, toggleClass, () => setTargetSubStatus(ds.target_id, s.subscription_id, canEnable ? 'ENABLED' : 'DISABLED')));
      }
      child.appendChild(actions);
      container.appendChild(child);
    }
  }

  async function setTargetSubStatus(tId, subId, status) {
    try { await api(`/targets/${tId}/subscriptions/${subId}/status`, { method: 'POST', body: JSON.stringify({ status }) }); }
    catch (e) { alert('Status change failed: ' + e.message); }
  }

  // ---- All Targets ----
  async function loadAllTargets() {
    try {
      const targets = await api('/targets');
      targetsCache = targets;
      lastTargetsCount = targets.length;
      renderAllTargetsTable();
    } catch (e) { console.error(e); }
  }

  function renderAllTargetsTable() {
    const list = $('targets-list');
    list.innerHTML = '';
    if (!targetsCache || !targetsCache.length) {
      list.innerHTML = '<p class="sub-row-meta" style="padding:16px;">No targets yet.</p>';
      return;
    }
    for (const t of targetsCache) {
      list.appendChild(buildAllTargetsRow(t));
    }
  }

  function buildAllTargetsRow(t) {
    const row = document.createElement('div');
    row.className = 'all-subs-row';
    const status = t.status || 'ENABLED';
    const isTerminal = status === 'DELETED';
    const canTrigger = ['ENABLED', 'ENQUEUED', 'IN_PROGRESS'].includes(status);
    const canEnable = status === 'ERROR' || status === 'DISABLED';
    const toggleLabel = canEnable ? 'Enable' : 'Disable';
    const toggleClass = canEnable ? 'btn-primary' : 'btn-destructive';
    const isExpanded = expandedTargets.has(t.target_id);
    const subCount = (t.subscriptions && t.subscriptions.length) || 0;
    row.innerHTML = `
      <span class="all-subs-col-name">
        <span class="sub-row-chevron" data-chevron style="margin-right:6px;">${isExpanded ? '▾' : '▸'}</span>
        ${escapeHtml(t.name)}
      </span>
      <span class="all-subs-col-plugin">${escapeHtml(t.service_display_name || t.service_name || '')}</span>
      <span class="all-subs-col-status"><span class="badge badge-${status}">${escapeHtml(status)}</span></span>
      <span class="all-subs-col-updated">${escapeHtml(relativeTime(t.last_updated))}</span>
      <span class="all-subs-col-edit"><button class="btn btn-primary" data-ds="${escapeHtml(t.target_id)}" data-svc="${escapeHtml(t.service_id)}" ${isTerminal ? 'disabled' : ''}>Edit</button></span>
      <span class="all-subs-col-toggle"><button class="btn ${toggleClass}" data-ds="${escapeHtml(t.target_id)}" data-target="${canEnable ? 'ENABLED' : 'DISABLED'}" ${isTerminal ? 'disabled' : ''}>${toggleLabel}</button></span>
      <span class="all-subs-col-update"><button class="btn btn-success" data-ds="${escapeHtml(t.target_id)}" ${canTrigger ? '' : 'disabled'}>Update</button></span>
      <span class="all-subs-col-children" style="display:${isExpanded ? 'block' : 'none'};"></span>
    `;
    const chevron = row.querySelector('[data-chevron]');
    const children = row.querySelector('.all-subs-col-children');
    if (subCount > 0) {
      chevron.style.cursor = 'pointer';
      chevron.addEventListener('click', () => {
        const open = children.style.display !== 'none';
        children.style.display = open ? 'none' : 'block';
        chevron.textContent = open ? '▸' : '▾';
        if (open) expandedTargets.delete(t.target_id);
        else expandedTargets.add(t.target_id);
      });
    } else {
      chevron.style.visibility = 'hidden';
    }
    buildTargetChildren(children, t);
    row.querySelectorAll('button').forEach(btn => {
      btn.addEventListener('click', async () => {
        const tId = btn.dataset.ds;
        if (btn.textContent === 'Edit') {
          const detail = await api(`/targets/${tId}`);
          openTargetForm(detail.service_id, detail);
        } else if (btn.textContent === 'Enable' || btn.textContent === 'Disable') {
          await api(`/targets/${tId}/status`, { method: 'POST', body: JSON.stringify({ status: btn.dataset.target }) });
        } else if (btn.textContent === 'Update') {
          await api(`/targets/${tId}/update`, { method: 'POST' });
        }
      });
    });
    return row;
  }

  // ---- Target Actions ----
  async function triggerTargetUpdate(tId) {
    try { await api(`/targets/${tId}/update`, { method: 'POST' }); }
    catch (e) { alert('Update failed: ' + e.message); }
  }

  async function setTargetStatus(tId, status) {
    try { await api(`/targets/${tId}/status`, { method: 'POST', body: JSON.stringify({ status }) }); }
    catch (e) { alert('Status change failed: ' + e.message); }
  }

  async function confirmDeleteTarget(ds) {
    if (!confirm(`Delete target '${ds.name}'?\n\nThe remote dataset will also be removed. This cannot be undone.`)) return;
    let deleted = false;
    try {
      await api(`/targets/${ds.target_id}`, { method: 'DELETE' });
      deleted = true;
    } catch (e) {
      const force = confirm(
        `Delete failed: ${e.message}\n\n` +
        `The target has been retained so you can retry later.\n\n` +
        `Delete the AutoKB records anyway? Any remote data would be left for manual cleanup.`
      );
      if (!force) return;
      try {
        await api(`/targets/${ds.target_id}?force=true`, { method: 'DELETE' });
        deleted = true;
      } catch (e2) {
        alert('Force delete failed: ' + e2.message);
        return;
      }
    }
    if (deleted) {
      if (currentView === 'destinations-detail' && currentSinkId) loadTargets(currentSinkId);
      if (currentView === 'data-targets') loadAllTargets();
    }
  }

  // ---- DKB Form ----
  let targetFormSinkId = null;
  let targetFormTargetId = null;
  let targetFormAllSubs = [];

  async function openTargetForm(serviceId, ds) {
    targetFormSinkId = serviceId;
    targetFormTargetId = ds ? ds.target_id : null;
    const services = await api('/sinks');
    const svc = services.find(s => s.service_id === serviceId);
    const svcName = svc ? (svc.display_name || svc.name) : '';
    $('target-form-title').textContent = ds ? `Edit Target (${svcName})` : `Create Data Target (${svcName})`;
    await buildTargetForm(ds, svc);
    $('target-form-modal').style.display = 'flex';
  }

  const TARGET_NAME_MAX_LEN = 255;
  const TARGET_NAME_RE = /^[a-zA-Z0-9.\-]+$/;

  function validateTargetName(name) {
    name = (name || '').trim();
    if (!name) return 'Target name is required.';
    if (name.length > TARGET_NAME_MAX_LEN) {
      return `Target name is too long (${name.length} chars; max ${TARGET_NAME_MAX_LEN}).`;
    }
    if (!TARGET_NAME_RE.test(name)) {
      return 'Use only letters, numbers, periods, and hyphens — no spaces or symbols.';
    }
    if (name.includes('..')) {
      return "Target name must not contain '..'.";
    }
    if (name.startsWith('.') || name.endsWith('.')) {
      return 'Target name must not start or end with a period.';
    }
    return null;
  }

  const SUBSCRIPTION_NAME_MAX_LEN = 255;
  const SUBSCRIPTION_NAME_RE = /^[a-zA-Z0-9\-]+$/;

  function validateSubscriptionName(name) {
    name = (name || '').trim();
    if (!name) return 'Subscription name is required.';
    if (name.length > SUBSCRIPTION_NAME_MAX_LEN) {
      return `Subscription name is too long (${name.length} chars; max ${SUBSCRIPTION_NAME_MAX_LEN}).`;
    }
    if (!SUBSCRIPTION_NAME_RE.test(name)) {
      return 'Use only letters, numbers, and hyphens — no periods, spaces, or symbols.';
    }
    return null;
  }

  async function buildTargetForm(ds, svc) {
    const fields = $('target-form-fields');
    fields.innerHTML = '';
    const isEdit = !!ds;
    // Name (editable only on create; immutable after creation)
    const nameDiv = document.createElement('div'); nameDiv.className = 'form-field';
    if (isEdit) {
      nameDiv.innerHTML = `<label>Name</label><div class="sub-row-meta" style="padding:8px 0;">${escapeHtml(ds.name)}</div><small class="sub-row-meta">Target name cannot be changed after creation.</small>`;
    } else {
      nameDiv.innerHTML = `<label>Name <span class="form-field-error">*</span></label>` +
        `<input type="text" name="target_name" required maxlength="${TARGET_NAME_MAX_LEN}" />` +
        `<small class="form-field-error" id="target-name-error" style="display:none"></small>`;
    }
    fields.appendChild(nameDiv);
    const nameInput = nameDiv.querySelector('input[name="target_name"]');
    if (nameInput) {
      const nameErr = nameDiv.querySelector('#target-name-error');
      const updateTargetNameValidation = () => {
        const err = validateTargetName(nameInput.value);
        if (err) {
          nameErr.textContent = err;
          nameErr.style.display = 'block';
          nameInput.classList.add('invalid');
        } else {
          nameErr.style.display = 'none';
          nameInput.classList.remove('invalid');
        }
      };
      nameInput.addEventListener('input', updateTargetNameValidation);
    }
    // API URL (pre-fill the service default on create, mirroring the source schema)
    const defaultApiUrl = (svc && svc.default_api_url) ? svc.default_api_url : '';
    const urlDiv = document.createElement('div'); urlDiv.className = 'form-field';
    urlDiv.innerHTML = `<label>API URL <span class="form-field-error">*</span></label><input type="text" name="api_url" value="${escapeHtml(ds ? ds.api_url : defaultApiUrl)}" required />`;
    fields.appendChild(urlDiv);
    // API Key (server-side env default resolves at recon when left blank)
    const hasKeyDefault = !!(svc && svc.has_api_key_default);
    const keyRequired = !isEdit && !hasKeyDefault;
    const keyPlaceholder = isEdit ? 'leave blank to keep existing' : (hasKeyDefault ? 'leave blank to use server default' : '');
    const keyDiv = document.createElement('div'); keyDiv.className = 'form-field';
    keyDiv.innerHTML = `<label>API Key ${keyRequired ? '<span class="form-field-error">*</span>' : ''}</label><input type="password" name="api_key" value="" placeholder="${keyPlaceholder}" />
    <small class="sub-row-meta">${isEdit ? 'Leave blank to keep the existing key.' : (hasKeyDefault ? 'Leave blank to use the server-configured default key.' : '')}</small>`;
    fields.appendChild(keyDiv);
    // target_extra_params
    const extraDiv = document.createElement('div'); extraDiv.className = 'form-field';
    extraDiv.innerHTML = `<label>Extra Params (JSON)</label><textarea name="target_extra_params" rows="4">${escapeHtml(ds ? JSON.stringify(ds.target_extra_params || {}, null, 2) : '{}')}</textarea>`;
    fields.appendChild(extraDiv);
    // Upload schedule window — available on BOTH create and edit.
    const schedRow = document.createElement('div'); schedRow.className = 'form-row';
    schedRow.innerHTML = `
      <div class="form-field"><label>Upload Window Start</label><input type="time" name="schedule_start" value="${escapeHtml(ds ? (ds.schedule_start || '') : '')}" /><small class="sub-row-meta">Only upload files to this target during this daily window. Leave both blank to always allow.</small></div>
      <div class="form-field"><label>Upload Window End</label><input type="time" name="schedule_end" value="${escapeHtml(ds ? (ds.schedule_end || '') : '')}" /></div>`;
    fields.appendChild(schedRow);
    // include_path_in_filename checkbox — creation only (changing after
    // uploads have been made would orphan existing remote file names).
    if (!ds) {
        const pathDiv = document.createElement('div'); pathDiv.className = 'form-field';
        pathDiv.innerHTML = `<label><input type="checkbox" name="include_path_in_filename" /> Include full directory structure in remote filename</label><small class="sub-row-meta">When enabled, the remote filename includes the plugin/subdirectory path: <code>autokb_{target}_{plugin}_{sub}_{basename}</code>.</small>`;
        fields.appendChild(pathDiv);
    }
    // Subscription transfer list
    const transferDiv = document.createElement('div'); transferDiv.className = 'form-field';
    transferDiv.innerHTML = `<label>Linked Subscriptions</label><div class="transfer-list"><div class="transfer-panel"><div class="transfer-header">Available</div><select id="target-available-subs" multiple></select></div><div class="transfer-buttons"><button type="button" class="btn btn-primary" id="target-transfer-right">&gt;&gt;</button><button type="button" class="btn btn-primary" id="target-transfer-left">&lt;&lt;</button></div><div class="transfer-panel"><div class="transfer-header">Linked</div><select id="target-linked-subs" multiple></select></div></div>`;
    fields.appendChild(transferDiv);
    // Populate subscription lists
    const allSubs = await api('/subscriptions');
    targetFormAllSubs = allSubs || [];
    const linkedIds = (ds && ds.subscriptions) ? ds.subscriptions.map(s => s.subscription_id) : [];
    populateTransferList(linkedIds);
  }

  function populateTransferList(linkedIds) {
    const avail = $('target-available-subs');
    const linked = $('target-linked-subs');
    avail.innerHTML = ''; linked.innerHTML = '';
    const linkedSet = new Set(linkedIds);
    for (const sub of targetFormAllSubs) {
      const opt = document.createElement('option');
      opt.value = sub.id;
      opt.textContent = sub.name;
      if (linkedSet.has(sub.id)) {
        linked.appendChild(opt);
      } else {
        avail.appendChild(opt);
      }
    }
    // Wire transfer buttons
    $('target-transfer-right').onclick = () => {
      const selected = Array.from(avail.selectedOptions);
      for (const opt of selected) { linked.appendChild(opt); }
    };
    $('target-transfer-left').onclick = () => {
      const selected = Array.from(linked.selectedOptions);
      for (const opt of selected) { avail.appendChild(opt); }
    };
  }

  $('target-form-cancel').addEventListener('click', () => { $('target-form-modal').style.display = 'none'; });

  $('target-form').addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const fd = new FormData($('target-form'));
    const isEdit = !!targetFormTargetId;
    let name;
    if (!isEdit) {
      name = fd.get('target_name');
      const nameInput = $('target-form').querySelector('input[name="target_name"]');
      const nameErr = validateTargetName(name);
      if (nameErr) {
        const errEl = $('target-name-error');
        if (errEl) { errEl.textContent = nameErr; errEl.style.display = 'block'; }
        if (nameInput) {
          nameInput.classList.add('invalid');
          nameInput.focus();
        }
        return;
      }
    }
    const apiUrl = fd.get('api_url');
    const apiKey = fd.get('api_key');
    let extra = fd.get('target_extra_params');
    try { extra = JSON.parse(extra); } catch (e) { extra = {}; }
    const linked = Array.from($('target-linked-subs').options).map(o => o.value);
    const body = { api_url: apiUrl, api_key: apiKey, target_extra_params: extra, subscription_ids: linked, schedule_start: fd.get('schedule_start') || '', schedule_end: fd.get('schedule_end') || '' };
    if (!targetFormTargetId) {
        body.include_path_in_filename = !!fd.get('include_path_in_filename');
    }
    if (name) body.name = name;
    try {
      if (targetFormTargetId) {
        await api(`/targets/${targetFormTargetId}`, { method: 'PUT', body: JSON.stringify(body) });
      } else {
        await api(`/sinks/${targetFormSinkId}/targets`, { method: 'POST', body: JSON.stringify(body) });
      }
      $('target-form-modal').style.display = 'none';
      if (currentView === 'destinations-detail' && currentSinkId) loadTargets(currentSinkId);
      if (currentView === 'data-targets') loadAllTargets();
    } catch (e) { alert('Save failed: ' + e.message); }
  });

  // ---- Target SSE handler ----
  function handleTargetUpdate(payload) {
    if (!payload || !payload.target_id) return;
    if (currentView === 'destinations-detail') {
      if (currentSinkId) loadTargets(currentSinkId);
    } else if (currentView === 'data-targets') {
      loadAllTargets();
    }
  }

  // ---- Dev Lab ----

  function showDevlabLanding() {
    $('devlab-landing').style.display = 'block';
    $('devlab-plugin-panel').style.display = 'none';
    $('devlab-destination-panel').style.display = 'none';
  }

  function showDevlabPanel(which) {
    $('devlab-landing').style.display = 'none';
    $('devlab-plugin-panel').style.display = (which === 'source') ? 'block' : 'none';
    $('devlab-destination-panel').style.display = (which === 'destination') ? 'block' : 'none';
  }

  $('devlab-tile-source').addEventListener('click', () => { location.hash = '#/devlab/source'; });
  $('devlab-tile-destination').addEventListener('click', () => { location.hash = '#/devlab/destination'; });
  $('devlab-back').addEventListener('click', () => { location.hash = '#/devlab'; });
  $('dest-devlab-back').addEventListener('click', () => { location.hash = '#/devlab'; });

  // ---- Plugin Development Guide ----
  let _cachedGuideMd = null;

  async function loadPluginGuide() {
    const contentDiv = $('devlab-guide-content');
    if (contentDiv.dataset.loaded) return;
    try {
      const resp = await fetch('/assets/plugin-development.md?v=38');
      if (!resp.ok) throw new Error('Not found');
      _cachedGuideMd = await resp.text();
      contentDiv.innerHTML = marked.parse(_cachedGuideMd);
      contentDiv.dataset.loaded = 'true';
    } catch (e) {
      contentDiv.innerHTML = '<p class="muted">Plugin development guide not available.</p>';
    }
  }

  $('devlab-guide-copy').addEventListener('click', async () => {
    if (!_cachedGuideMd) {
      try {
        const resp = await fetch('/assets/plugin-development.md?v=38');
        _cachedGuideMd = await resp.text();
      } catch (e) { return; }
    }
    const btn = $('devlab-guide-copy');
    try {
      await navigator.clipboard.writeText(_cachedGuideMd);
    } catch (e) {
      const ta = document.createElement('textarea');
      ta.value = _cachedGuideMd;
      ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
    const orig = btn.textContent;
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = orig; }, 2000);
  });

  // ---- Sink Development Guide ----
  let _cachedSinkGuideMd = null;

  async function loadSinkGuide() {
    const contentDiv = $('dest-devlab-guide-content');
    if (contentDiv.dataset.loaded) return;
    try {
      const resp = await fetch('/assets/sink-development.md?v=1');
      if (!resp.ok) throw new Error('Not found');
      _cachedSinkGuideMd = await resp.text();
      contentDiv.innerHTML = marked.parse(_cachedSinkGuideMd);
      contentDiv.dataset.loaded = 'true';
    } catch (e) {
      contentDiv.innerHTML = '<p class="muted">Sink development guide not available.</p>';
    }
  }

  $('dest-devlab-guide-copy').addEventListener('click', async () => {
    if (!_cachedSinkGuideMd) {
      try {
        const resp = await fetch('/assets/sink-development.md?v=1');
        _cachedSinkGuideMd = await resp.text();
      } catch (e) { return; }
    }
    const btn = $('dest-devlab-guide-copy');
    try {
      await navigator.clipboard.writeText(_cachedSinkGuideMd);
    } catch (e) {
      const ta = document.createElement('textarea');
      ta.value = _cachedSinkGuideMd;
      ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
    const orig = btn.textContent;
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = orig; }, 2000);
  });

  async function loadDevlabForEdit(pluginId) {
    devlabEditPluginId = pluginId;
    const banner = $('devlab-edit-banner');
    const nameInput = $('devlab-name');
    const codeInput = $('devlab-code');
    const iconInput = $('devlab-icon');
    nameInput.value = pluginId;
    nameInput.readOnly = true;
    codeInput.value = 'Loading…';
    iconInput.value = '';
    banner.style.display = 'block';
    banner.innerHTML = `<strong>Editing existing plugin: ${escapeHtml(pluginId)}</strong> &mdash; changes to the config (schema) will be rejected. The plugin's source code, getData() implementation, and metadata can be updated, but the JSON schema returned by get_schema() must remain identical.`;
    try {
      const r = await api(`/dev_lab/load/${encodeURIComponent(pluginId)}`);
      if (r.ok && r.code != null) {
        codeInput.value = r.code;
        $('devlab-display-name').value = r.display_name || '';
      } else {
        codeInput.value = '';
        $('devlab-result').className = 'devlab-result error';
        $('devlab-result').textContent = '✗ Could not load plugin source.';
      }
    } catch (e) {
      codeInput.value = '';
      $('devlab-result').className = 'devlab-result error';
      $('devlab-result').textContent = '✗ ' + e.message;
    }
  }

  function resetDevlabToCreateMode() {
    devlabEditPluginId = null;
    const banner = $('devlab-edit-banner');
    banner.style.display = 'none';
    banner.innerHTML = '';
    const nameInput = $('devlab-name');
    nameInput.readOnly = false;
    nameInput.value = '';
    $('devlab-display-name').value = '';
    $('devlab-code').value = '';
    $('devlab-icon').value = '';
    $('devlab-result').className = 'devlab-result';
    $('devlab-result').textContent = '';
  }

  // ---- Remote DKB Developer Lab ----

  let sinkDevlabEditName = null;

  async function loadSinkDevlabForEdit(serviceName) {
    sinkDevlabEditName = serviceName;
    const banner = $('dest-devlab-edit-banner');
    const nameInput = $('dest-devlab-name');
    const codeInput = $('dest-devlab-code');
    const iconInput = $('dest-devlab-icon');
    nameInput.value = serviceName;
    nameInput.readOnly = true;
    codeInput.value = 'Loading…';
    iconInput.value = '';
    banner.style.display = 'block';
    banner.innerHTML = `<strong>Editing existing sink: ${escapeHtml(serviceName)}</strong> &mdash; the service name is locked; update the code and metadata below.`;
    try {
      const r = await api(`/sink_dev_lab/load/${encodeURIComponent(serviceName)}`);
      if (r.ok && r.code != null) {
        codeInput.value = r.code;
        $('dest-devlab-display-name').value = r.display_name || '';
      } else {
        codeInput.value = '';
        $('dest-devlab-result').className = 'devlab-result error';
        $('dest-devlab-result').textContent = '✗ Could not load Destination source.';
      }
    } catch (e) {
      codeInput.value = '';
      $('dest-devlab-result').className = 'devlab-result error';
      $('dest-devlab-result').textContent = '✗ ' + e.message;
    }
  }

  function resetSinkDevlabToCreateMode() {
    sinkDevlabEditName = null;
    const banner = $('dest-devlab-edit-banner');
    banner.style.display = 'none';
    banner.innerHTML = '';
    const nameInput = $('dest-devlab-name');
    nameInput.readOnly = false;
    nameInput.value = '';
    $('dest-devlab-display-name').value = '';
    $('dest-devlab-code').value = '';
    $('dest-devlab-icon').value = '';
    $('dest-devlab-result').className = 'devlab-result';
    $('dest-devlab-result').textContent = '';
  }

  $('dest-devlab-test-btn').addEventListener('click', async () => {
    const name = $('dest-devlab-name').value;
    const display_name = $('dest-devlab-display-name').value;
    const code = $('dest-devlab-code').value;
    const result = $('dest-devlab-result');
    result.className = 'devlab-result';
    result.textContent = 'Testing...';
    try {
      const r = await api('/sink_dev_lab/validate', { method: 'POST', body: JSON.stringify({ name, display_name, code }) });
      if (r.ok) {
        result.classList.add('success');
        result.textContent = '✓ Validation passed';
      } else {
        result.classList.add('error');
        result.textContent = '✗ ' + r.error;
      }
    } catch (e) {
      result.classList.add('error');
      result.textContent = '✗ ' + e.message;
    }
  });

  $('dest-devlab-save-btn').addEventListener('click', async () => {
    const name = $('dest-devlab-name').value;
    const display_name = $('dest-devlab-display-name').value;
    const code = $('dest-devlab-code').value;
    const iconFile = $('dest-devlab-icon').files[0];
    const result = $('dest-devlab-result');
    result.className = 'devlab-result';
    result.textContent = 'Saving...';
    let icon_b64 = null;
    if (iconFile) {
      icon_b64 = await new Promise((res, rej) => {
        const reader = new FileReader();
        reader.onload = () => res(reader.result.split(',')[1]);
        reader.onerror = rej;
        reader.readAsDataURL(iconFile);
      });
    }
    try {
      const r = await api('/sink_dev_lab/save', { method: 'POST', body: JSON.stringify({ name, display_name, code, icon_base64: icon_b64 }) });
      result.classList.add('success');
      if (r.mode === 'edit') {
        result.textContent = '✓ Destination updated. The new code will be picked up by the file watcher within seconds.';
      } else {
        result.textContent = '✓ Saved. Destination will appear in Data Destinations within seconds.';
      }
    } catch (e) {
      result.classList.add('error');
      result.textContent = '✗ ' + e.message;
    }
  });

  $('devlab-test-btn').addEventListener('click', async () => {
    const name = $('devlab-name').value;
    const display_name = $('devlab-display-name').value;
    const code = $('devlab-code').value;
    const result = $('devlab-result');
    result.className = 'devlab-result';
    result.textContent = 'Testing...';
    try {
      const r = await api('/dev_lab/validate', { method: 'POST', body: JSON.stringify({ name, display_name, code }) });
      if (r.ok) {
        result.classList.add('success');
        result.textContent = '✓ Validation passed';
      } else {
        result.classList.add('error');
        result.textContent = '✗ ' + r.error;
      }
    } catch (e) {
      result.classList.add('error');
      result.textContent = '✗ ' + e.message;
    }
  });

  $('devlab-save-btn').addEventListener('click', async () => {
    const name = $('devlab-name').value;
    const display_name = $('devlab-display-name').value;
    const code = $('devlab-code').value;
    const iconFile = $('devlab-icon').files[0];
    const result = $('devlab-result');
    result.className = 'devlab-result';
    result.textContent = 'Saving...';
    let icon_b64 = null;
    if (iconFile) {
      icon_b64 = await new Promise((res, rej) => {
        const reader = new FileReader();
        reader.onload = () => res(reader.result.split(',')[1]);
        reader.onerror = rej;
        reader.readAsDataURL(iconFile);
      });
    }
    try {
      const r = await api('/dev_lab/save', { method: 'POST', body: JSON.stringify({ name, display_name, code, icon_base64: icon_b64 }) });
      result.classList.add('success');
      if (r.mode === 'edit') {
        result.textContent = '✓ Plugin updated. The new code will be picked up on the next run (worker loads plugins fresh per job).';
      } else {
        result.textContent = '✓ Saved. Plugin will be available within seconds.';
      }
    } catch (e) {
      result.classList.add('error');
      result.textContent = '✗ ' + e.message;
    }
  });

  // ---- SSE ----
  function startSSE() {
    // Close any existing connection first.
    if (sseSource) {
      try { sseSource.close(); } catch (e) {}
      sseSource = null;
    }
    console.log('[SSE] connecting to /api/events');
    sseSource = new EventSource('/api/events');
    sseSource.onopen = () => {
      console.log('[SSE] connection open, readyState=' + sseSource.readyState);
    };
    sseSource.onmessage = (ev) => {
      let data;
      try { data = JSON.parse(ev.data); }
      catch (e) { console.warn('[SSE] malformed event', e); return; }
      if (!data || !data.type) return;
      console.log('[SSE] event:', data.type, data.data && data.data.id);
      if (data.type === 'subscription_update') {
        handleSubscriptionUpdate(data.data);
      } else if (data.type === 'subscription_deleted') {
        handleSubscriptionDeleted(data.data);
      } else if (data.type === 'target_update') {
        handleTargetUpdate(data.data);
      } else if (data.type === 'target_deleted') {
        if (currentView === 'data-targets') loadAllTargets();
        if (currentView === 'destinations-detail' && currentSinkId) loadTargets(currentSinkId);
      }
      // 'snapshot_complete' is informational; no action needed.
    };
    sseSource.onerror = (ev) => {
      // EventSource auto-reconnects by default. Only intervene if the
      // connection is permanently closed (e.g., server refused or max
      // retries exceeded).
      console.warn('[SSE] error, readyState=' + (sseSource ? sseSource.readyState : 'null'));
      if (sseSource && sseSource.readyState === EventSource.CLOSED) {
        console.log('[SSE] permanently closed, reconnecting in 2s');
        setTimeout(startSSE, 2000);
      }
    };
  }

  function handleSubscriptionUpdate(sub) {
    if (!sub || !sub.id) return;
    // Capture previous status from the all-subs cache BEFORE updating it,
    // so we can detect a run-completion transition for the activity counter.
    const ai = allSubsCache.findIndex(s => s.id === sub.id);
    const prevStatus = ai >= 0 ? allSubsCache[ai].status : null;

    // Always update the per-plugin in-memory cache so any future view-switch is correct.
    const cached = subscriptionsCache[sub.plugin_id] || [];
    const idx = cached.findIndex(s => s.id === sub.id);
    if (idx >= 0) cached[idx] = sub; else cached.push(sub);
    subscriptionsCache[sub.plugin_id] = cached;

    // Update the all-subs table cache (used by the cross-plugin list).
    if (ai >= 0) allSubsCache[ai] = sub; else allSubsCache.push(sub);

    // If this update represents a completed run (running -> done transition),
    // bump the 24h activity counter so the all-subs table reflects it live
    // without a page refresh.
    const runningStates = ['IN_PROGRESS', 'ENQUEUED'];
    const completedStates = ['ENABLED', 'ERROR', 'DISABLED'];
    if (prevStatus && runningStates.includes(prevStatus) && completedStates.includes(sub.status)) {
      allSubsActivity[sub.id] = (allSubsActivity[sub.id] || 0) + 1;
    }

    if (currentView === 'subscriptions') {
      // Use currentPlugin.plugin_id (set when the subscriptions view loaded)
      // — currentPluginId may not be set yet during the initial load.
      if (currentPlugin && sub.plugin_id === currentPlugin.plugin_id) {
        updateSubscriptionRow(sub, currentPlugin);
        updateDeletePluginBtnState();
      }
    } else if (currentView === 'all-subscriptions') {
      renderAllSubscriptionsTable();
    } else if (currentView === 'dashboard') {
      // The dashboard reads straight from allSubsCache / allSubsActivity
      // via renderDashboardStats, so a re-render picks up the new value
      // (status change affects the By-Status mini-bar, the 24h counter
      // bumps on run-completion, etc.). Health keeps its own 30s timer.
      renderDashboardStats();
    }
  }

  function handleSubscriptionDeleted(payload) {
    if (!payload || !payload.id) return;
    const cached = subscriptionsCache[payload.plugin_id] || [];
    subscriptionsCache[payload.plugin_id] = cached.filter(s => s.id !== payload.id);
    allSubsCache = allSubsCache.filter(s => s.id !== payload.id);
    delete allSubsActivity[payload.id];
    if (currentView === 'subscriptions') {
      if (currentPlugin && payload.plugin_id === currentPlugin.plugin_id) {
        removeSubscriptionRow(payload.id);
        updateDeletePluginBtnState();
      }
    } else if (currentView === 'all-subscriptions') {
      renderAllSubscriptionsTable();
    } else if (currentView === 'dashboard') {
      renderDashboardStats();
    }
  }

  // ---- Boot ----
  (async () => {
    if (await checkAuth()) {
      parseHash();
      startSSE();
      // Refresh activity when navigating to it
      setInterval(() => {
        if (currentView === 'activity') loadActivity();
        if (currentView === 'subscriptions' && currentPluginId) {
          // Just refresh activity counts
        }
      }, 5000);
    }
  })();
})();
