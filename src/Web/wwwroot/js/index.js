(() => {
  const navList = document.getElementById('nav-list');
  const tabHeader = document.getElementById('tab-header');
  const tabContent = document.getElementById('tab-content');
  const addTabButton = document.getElementById('tab-add');

  if (!navList || !tabHeader || !tabContent || !addTabButton) {
    return;
  }

  // Mock data for tabs and topics
  const navItems = [];
  const tabState = {
    activeId: 'topic-view',
    tabs: [
      { id: 'topic-view', title: 'Topics', closable: false, render: renderTopicView }
    ]
  };

  let topics = [];
  let currentFilter = { name: '' };

  // Load topics from API (with fallback)
  const loadTopics = (filter) => {
    const params = new URLSearchParams();
    if (filter?.name) params.append('name', filter.name);

    return fetch('/api/topics?' + params.toString())
      .then(r => r.json())
      .then(data => {
        topics = data ?? [];
        if (tabState.activeId === 'topic-view') {
          renderTabs();
        }
      })
      .catch(() => {
        topics = [
          { name: 'payments', partitions: 12, messages: 152340, retentionDays: 7 },
          { name: 'notifications', partitions: 8, messages: 83412, retentionDays: 3 },
          { name: 'orders', partitions: 6, messages: 45012, retentionDays: 14 },
          { name: 'user-updates', partitions: 4, messages: 32001, retentionDays: 10 },
          { name: 'audit-log', partitions: 3, messages: 9512, retentionDays: 30 }
        ].filter(t =>
          !filter?.name ||
          t.name.toLowerCase().includes(filter.name.toLowerCase())
        );
        if (tabState.activeId === 'topic-view') {
          renderTabs();
        }
      });
  };

  // --- Navigation ---
  fetch('/api/nav')
    .then(r => r.json())
    .then(data => {
      navItems.splice(0, navItems.length, ...data);
      renderNav();
    })
    .catch(() => {
      navItems.splice(0, navItems.length,
        { id: 'overview', title: 'Overview' },
        { id: 'topics', title: 'Topics' },
        { id: 'settings', title: 'Settings' }
      );
      renderNav();
    });

  function renderNav() {
    navList.innerHTML = '';
    navItems.forEach(item => {
      const li = document.createElement('li');
      li.textContent = item.title;
      li.dataset.navId = item.id;
      li.className = 'nav-item';
      li.addEventListener('click', () => openTab(item.id, item.title));
      navList.appendChild(li);
    });
  }

  // --- Tabs ---
  function renderTabs() {
    tabHeader.querySelectorAll('.tab').forEach(t => t.remove());
    tabState.tabs.forEach(tab => {
      const btn = document.createElement('button');
      btn.className = 'tab' + (tab.id === tabState.activeId ? ' active' : '');
      btn.textContent = tab.title;
      btn.dataset.tabId = tab.id;
      btn.addEventListener('click', () => activateTab(tab.id));

      if (tab.closable) {
        const close = document.createElement('span');
        close.textContent = '×';
        close.className = 'tab-close';
        close.addEventListener('click', (e) => {
          e.stopPropagation();
          closeTab(tab.id);
        });
        btn.appendChild(close);
      }
      tabHeader.insertBefore(btn, addTabButton);
    });
    renderTabContent();
  }

  function renderTabContent() {
    tabContent.innerHTML = '';
    const activeTab = tabState.tabs.find(t => t.id === tabState.activeId);
    if (!activeTab) return;

    const pane = document.createElement('div');
    pane.className = 'tab-pane active';
    pane.dataset.tabId = activeTab.id;

    if (activeTab.render) {
      activeTab.render(pane, activeTab);
    } else {
      pane.textContent = `${activeTab.title} content`;
    }
    tabContent.appendChild(pane);
  }

  function activateTab(id) {
    tabState.activeId = id;
    renderTabs();
  }

  function openTab(id, title) {
    const exists = tabState.tabs.find(t => t.id === id);
    if (!exists) {
      tabState.tabs.push({ id, title, closable: true });
    }
    activateTab(id);
  }

  function closeTab(id) {
    const idx = tabState.tabs.findIndex(t => t.id === id && t.closable);
    if (idx === -1) return;
    tabState.tabs.splice(idx, 1);
    if (tabState.activeId === id) {
      tabState.activeId = tabState.tabs[Math.max(0, idx - 1)].id;
    }
    renderTabs();
  }

  addTabButton.addEventListener('click', () => {
    const id = `tab-${Date.now()}`;
    tabState.tabs.push({ id, title: 'New Tab', closable: true });
    activateTab(id);
  });

  // --- Topic view ---
  function renderTopicView(container) {
    container.innerHTML = '';
    container.appendChild(document.querySelector('[data-tab-content="topic-view"]').cloneNode(true));
    const filterInput = container.querySelector('#topic-filter-input');
    const filterBtn = container.querySelector('#topic-filter-btn');
    const tbody = container.querySelector('#topic-table tbody');

    const renderRows = (filter = '') => {
      tbody.innerHTML = '';
      topics
        .filter(t => t.name.toLowerCase().includes(filter.toLowerCase()))
        .forEach(t => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${t.name}</td>
            <td>${t.partitions}</td>
            <td>${t.messages.toLocaleString()}</td>
            <td>${t.retentionDays}</td>
          `;
          tbody.appendChild(tr);
        });
    };

    const applyFilter = () => {
      currentFilter.name = filterInput.value;
      loadTopics(currentFilter).then(() => renderRows(filterInput.value));
    };

    filterBtn.addEventListener('click', applyFilter);
    filterInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') {
        applyFilter();
      }
    });

    renderRows(filterInput.value);
  }

  // initial render
  renderTabs();
  loadTopics(currentFilter);
})();

