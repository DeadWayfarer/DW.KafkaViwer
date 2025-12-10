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
    activeId: 'topic-list-view',
    tabs: [
      { id: 'topic-list-view', title: 'Topics', closable: false, render: renderTopicView }
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
        if (tabState.activeId === 'topic-list-view') {
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
        if (tabState.activeId === 'topic-list-view') {
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
        { id: 'topic-list-view', title: 'Topics' },
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
      li.addEventListener('click', () => {
        if (item.id === 'brokers') {
          openTab('broker-view', 'Brokers', { render: renderBrokerView, closable: true });
        } else if (item.id === 'topic-list-view') {
          openTab('topic-list-view', 'Topics', { render: renderTopicView, closable: false });
        } else {
          openTab(item.id, item.title);
        }
      });
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

  function openTab(id, title, options = {}) {
    const exists = tabState.tabs.find(t => t.id === id);
    if (!exists) {
      tabState.tabs.push({
        id,
        title,
        closable: options.closable ?? true,
        render: options.render,
        topic: options.topic
      });
    } else {
      if (options.topic) exists.topic = options.topic;
      if (options.render) exists.render = options.render;
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
    container.appendChild(document.querySelector('[data-tab-content="topic-list-view"]').cloneNode(true));
    const filterInput = container.querySelector('#topic-filter-input');
    const filterBtn = container.querySelector('#topic-filter-btn');
    const tbody = container.querySelector('#topic-table tbody');

    filterInput.value = currentFilter.name ?? '';

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
          tr.addEventListener('click', () => {
            const tabId = `message-${t.name}`;
            openTab(tabId, `Messages: ${t.name}`, { render: renderMessageView, topic: t.name });
          });
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

  // --- Message view ---
  function renderMessageView(container, tab) {
    const topicName = tab.topic || tab.title.replace('Messages: ', '');
    container.innerHTML = '';
    container.appendChild(document.querySelector('[data-tab-content="message-view"]').cloneNode(true));

    const meta = container.querySelector('#message-meta');
    const tbody = container.querySelector('#message-table tbody');
    const searchTypeEl = container.querySelector('#msg-search-type');
    const limitEl = container.querySelector('#msg-limit');
    const fromEl = container.querySelector('#msg-from');
    const toEl = container.querySelector('#msg-to');
    const filterEl = container.querySelector('#msg-filter');
    const searchBtn = container.querySelector('#msg-search-btn');
    const createBtn = container.querySelector('#msg-create-btn');
    const consumersBtn = container.querySelector('#msg-consumers-btn');

    meta.textContent = `Топик: ${topicName}`;

    tab.messageFilter = tab.messageFilter || {
      searchType: 'newest',
      limit: 20,
      from: '',
      to: '',
      query: ''
    };

    // initialize UI from state
    searchTypeEl.value = tab.messageFilter.searchType || 'newest';
    limitEl.value = tab.messageFilter.limit ?? 20;
    fromEl.value = tab.messageFilter.from || '';
    toEl.value = tab.messageFilter.to || '';
    filterEl.value = tab.messageFilter.query || '';

    const renderRows = (rows) => {
      tbody.innerHTML = '';
      rows.forEach(m => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${m.partition}</td>
          <td>${m.offset}</td>
          <td>${m.key}</td>
          <td>${m.value}</td>
          <td>${new Date(m.timestampUtc).toLocaleString()}</td>
        `;
        tbody.appendChild(tr);
      });
    };

    const loadMessages = () => {
      const params = new URLSearchParams();
      params.append('topic', topicName);
      if (searchTypeEl.value) params.append('searchType', searchTypeEl.value);
      if (limitEl.value) params.append('limit', limitEl.value);
      if (fromEl.value) params.append('from', fromEl.value);
      if (toEl.value) params.append('to', toEl.value);
      if (filterEl.value) params.append('query', filterEl.value);

      tab.messageFilter = {
        searchType: searchTypeEl.value,
        limit: limitEl.value,
        from: fromEl.value,
        to: toEl.value,
        query: filterEl.value
      };

      fetch('/api/messages?' + params.toString())
        .then(r => r.json())
        .then(data => renderRows(data ?? []))
        .catch(() => {
          const fallback = [{
            partition: 0,
            offset: 0,
            key: 'n/a',
            value: `No data for ${topicName}`,
            timestampUtc: new Date().toISOString()
          }];
          renderRows(fallback);
        });
    };

    searchBtn.addEventListener('click', loadMessages);
    filterEl.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') loadMessages();
    });

    // Placeholder for create message
    createBtn.addEventListener('click', () => {
      alert('Создание сообщения: пока мок-реализация');
    });

    consumersBtn.addEventListener('click', () => {
      const tabId = `consumers-${topicName}`;
      openTab(tabId, `Consumers: ${topicName}`, { render: renderConsumerView, topic: topicName });
    });

    loadMessages();
  }

  // --- Consumer view ---
  function renderConsumerView(container, tab) {
    const topicName = tab.topic || '';
    container.innerHTML = '';
    container.appendChild(document.querySelector('[data-tab-content="consumer-view"]').cloneNode(true));

    const tbody = container.querySelector('#consumer-table tbody');
    const refreshBtn = container.querySelector('#consumer-refresh-btn');
    const statusEl = container.querySelector('#consumer-status');

    const renderRows = (rows) => {
      tbody.innerHTML = '';
      rows.forEach(m => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${m.group}</td>
          <td>${m.member}</td>
          <td>${m.lag}</td>
          <td>${m.status}</td>
        `;
        tbody.appendChild(tr);
      });
    };

    const loadConsumers = () => {
      if (statusEl) statusEl.textContent = 'Загрузка...';
      fetch('/api/consumers?topic=' + encodeURIComponent(topicName))
        .then(r => r.json())
        .then(data => {
          renderRows(data ?? []);
          if (statusEl) statusEl.textContent = '';
        })
        .catch(() => {
          const mock = [
            { group: `${topicName}-grp`, member: 'consumer-1', lag: 12, status: 'Active' },
            { group: `${topicName}-grp`, member: 'consumer-2', lag: 3, status: 'Active' },
            { group: `${topicName}-grp`, member: 'consumer-3', lag: 25, status: 'Rebalancing' }
          ];
          renderRows(mock);
          if (statusEl) statusEl.textContent = 'Показаны мок-данные';
        });
    };

    refreshBtn.addEventListener('click', loadConsumers);

    loadConsumers();
  }

  // --- Broker view ---
  function renderBrokerView(container) {
    container.innerHTML = '';
    container.appendChild(document.querySelector('[data-tab-content="broker-view"]').cloneNode(true));

    const tbody = container.querySelector('#broker-table tbody');
    const addBtn = container.querySelector('#broker-add-btn');

    // Create modal dynamically
    let modal = document.getElementById('broker-modal');
    if (!modal) {
      modal = document.createElement('div');
      modal.id = 'broker-modal';
      modal.className = 'modal-overlay';
      modal.style.display = 'none';
      modal.innerHTML = `
        <div class="modal-content glass-panel">
          <div class="modal-header">
            <h3 id="broker-modal-title">Добавить брокер</h3>
            <button type="button" class="modal-close" id="broker-modal-close">×</button>
          </div>
          <div class="modal-body">
            <form id="broker-form">
              <input type="hidden" id="broker-id" />
              <div class="form-group">
                <label class="form-label" for="broker-host">Host</label>
                <input class="form-control" id="broker-host" type="text" required />
              </div>
              <div class="form-group">
                <label class="form-label" for="broker-port">Port</label>
                <input class="form-control" id="broker-port" type="number" min="1" max="65535" required />
              </div>
              <div class="form-group">
                <label class="form-label" for="broker-status">Status</label>
                <select class="form-select" id="broker-status" required>
                  <option value="Active">Active</option>
                  <option value="Inactive">Inactive</option>
                  <option value="Connecting">Connecting</option>
                  <option value="Error">Error</option>
                </select>
              </div>
            </form>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-outline-secondary" id="broker-modal-cancel">Отмена</button>
            <button type="button" class="btn btn-primary" id="broker-modal-apply">Применить</button>
          </div>
        </div>
      `;
      document.body.appendChild(modal);
    }

    const modalTitle = document.getElementById('broker-modal-title');
    const modalClose = document.getElementById('broker-modal-close');
    const modalCancel = document.getElementById('broker-modal-cancel');
    const modalApply = document.getElementById('broker-modal-apply');
    const brokerForm = document.getElementById('broker-form');
    const brokerIdInput = document.getElementById('broker-id');
    const brokerHostInput = document.getElementById('broker-host');
    const brokerPortInput = document.getElementById('broker-port');
    const brokerStatusInput = document.getElementById('broker-status');

    let currentBrokers = [];

    const openModal = (broker = null) => {
      if (broker) {
        modalTitle.textContent = 'Изменить брокер';
        brokerIdInput.value = broker.id;
        brokerHostInput.value = broker.host;
        brokerPortInput.value = broker.port;
        brokerStatusInput.value = broker.status;
      } else {
        modalTitle.textContent = 'Добавить брокер';
        brokerIdInput.value = '';
        brokerHostInput.value = '';
        brokerPortInput.value = '';
        brokerStatusInput.value = 'Active';
      }
      modal.style.display = 'flex';
    };

    const closeModal = () => {
      modal.style.display = 'none';
      brokerForm.reset();
    };

    modalClose.addEventListener('click', closeModal);
    modalCancel.addEventListener('click', closeModal);
    modal.addEventListener('click', (e) => {
      if (e.target === modal) {
        closeModal();
      }
    });

    modalApply.addEventListener('click', () => {
      if (!brokerForm.checkValidity()) {
        brokerForm.reportValidity();
        return;
      }

      const brokerData = {
        id: brokerIdInput.value ? parseInt(brokerIdInput.value) : 0,
        host: brokerHostInput.value.trim(),
        port: parseInt(brokerPortInput.value),
        status: brokerStatusInput.value
      };

      const isEdit = brokerIdInput.value !== '';
      const url = isEdit ? '/api/brokers' : '/api/brokers';
      const method = isEdit ? 'PUT' : 'POST';

      fetch(url, {
        method: method,
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(brokerData)
      })
        .then(r => {
          if (r.ok) {
            closeModal();
            loadBrokers();
          } else {
            alert('Ошибка при сохранении брокера');
          }
        })
        .catch(() => {
          alert('Ошибка при сохранении брокера');
        });
    });

    const renderRows = (rows) => {
      currentBrokers = rows;
      tbody.innerHTML = '';
      rows.forEach(b => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${b.id}</td>
          <td>${b.host}</td>
          <td>${b.port}</td>
          <td>${b.status}</td>
          <td>
            <button class="btn-icon btn-edit" data-broker-id="${b.id}" title="Изменить">✏️</button>
            <button class="btn-icon btn-delete" data-broker-id="${b.id}" title="Удалить">🗑️</button>
          </td>
        `;
        tbody.appendChild(tr);
      });

      // Add event listeners for action buttons
      tbody.querySelectorAll('.btn-edit').forEach(btn => {
        btn.addEventListener('click', (e) => {
          const brokerId = parseInt(e.target.dataset.brokerId);
          const broker = rows.find(b => b.id === brokerId);
          if (broker) {
            openModal(broker);
          }
        });
      });

      tbody.querySelectorAll('.btn-delete').forEach(btn => {
        btn.addEventListener('click', (e) => {
          const brokerId = parseInt(e.target.dataset.brokerId);
          const broker = rows.find(b => b.id === brokerId);
          if (broker && confirm(`Удалить брокер ${broker.host}:${broker.port}?`)) {
            fetch(`/api/brokers/${brokerId}`, {
              method: 'DELETE'
            })
              .then(r => {
                if (r.ok) {
                  loadBrokers();
                } else {
                  alert('Ошибка при удалении брокера');
                }
              })
              .catch(() => {
                alert('Ошибка при удалении брокера');
              });
          }
        });
      });
    };

    const loadBrokers = () => {
      fetch('/api/brokers')
        .then(r => r.json())
        .then(data => {
          renderRows(data ?? []);
        })
        .catch(() => {
          const mock = [
            { id: 1, host: 'localhost', port: 9092, status: 'Active' },
            { id: 2, host: 'localhost', port: 9093, status: 'Active' },
            { id: 3, host: 'localhost', port: 9094, status: 'Inactive' }
          ];
          renderRows(mock);
        });
    };

    addBtn.addEventListener('click', () => {
      openModal();
    });

    loadBrokers();
  }
})();

