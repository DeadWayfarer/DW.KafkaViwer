


(() => {
  const navList = document.getElementById('nav-list');
  const tabHeader = document.getElementById('tab-header');
  const tabContent = document.getElementById('tab-content');

  if (!navList || !tabHeader || !tabContent) {
    return;
  }

  // Mock data for tabs and topics
  const navItems = [];
  const tabState = {
    activeId: 'topic-list-view',
    tabs: [
      { id: 'topic-list-view', title: 'Топики', closable: false, render: renderTopicView }
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
        { id: 'overview', title: 'Обзор' },
        { id: 'topic-list-view', title: 'Топики' },
        { id: 'settings', title: 'Настройки' }
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
          openTab('broker-view', 'Брокеры', { render: renderBrokerView, closable: true });
        } else if (item.id === 'topic-list-view') {
          openTab('topic-list-view', 'Топики', { render: renderTopicView, closable: false });
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

      // Close tab on middle mouse button click
      btn.addEventListener('auxclick', (e) => {
        if (e.button === 1 && tab.closable) {
          e.preventDefault();
          e.stopPropagation();
          closeTab(tab.id);
        }
      });

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
      tabHeader.appendChild(btn);
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
        topic: options.topic,
        brokerId: options.brokerId
      });
    } else {
      if (options.topic) exists.topic = options.topic;
      if (options.render) exists.render = options.render;
      if (options.brokerId !== undefined) exists.brokerId = options.brokerId;
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
          const brokerName = t.brokerName ?? t.BrokerName ?? t.brokername ?? 'Неизвестно';
          const messagesCell = (t.messages === null || t.messages === undefined)
            ? '<span class="loading-icon" title="Загрузка...">⏳</span>'
            : t.messages.toLocaleString();

          tr.innerHTML = `
            <td>${brokerName}</td>
            <td>${t.name}</td>
            <td>${t.partitions}</td>
            <td>${messagesCell}</td>
            <td>${t.retentionDays}</td>
          `;
          tr.addEventListener('click', (e) => {
            // Remove active class from all rows
            tbody.querySelectorAll('tr').forEach(row => row.classList.remove('active'));
            // Add active class to clicked row
            tr.classList.add('active');
            
            const tabId = `message-${t.name}`;
            // Try different property names for brokerId and brokerName
            const brokerId = t.brokerId ?? t.BrokerId ?? t.brokerid;
            const brokerName = t.brokerName ?? t.BrokerName ?? t.brokername ?? '';
            console.log('Topic data:', t, 'BrokerId:', brokerId, 'BrokerName:', brokerName);
            if (!brokerId && brokerId !== 0) {
              console.error('BrokerId not found for topic:', t);
              alert('Ошибка: Не удалось определить ID брокера для топика. Проверьте консоль для деталей.');
              return;
            }
            openTab(tabId, `Сообщения: ${t.name}`, { render: renderMessageView, topic: t.name, brokerId: brokerId, brokerName: brokerName });
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

  // --- Notification system ---
  function showNotification(message, type = 'info') {
    // Remove existing notifications
    const existingNotifications = document.querySelectorAll('.notification');
    existingNotifications.forEach(n => n.remove());
    
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.innerHTML = `
      <div class="notification-content">
        <span class="notification-icon">${type === 'success' ? '✓' : type === 'error' ? '✕' : 'ℹ'}</span>
        <span class="notification-message">${message}</span>
      </div>
    `;
    
    document.body.appendChild(notification);
    
    // Trigger animation by adding active class
    requestAnimationFrame(() => {
      notification.classList.add('notification-active');
    });
    
    // Auto-remove after 3 seconds
    setTimeout(() => {
      notification.classList.remove('notification-active');
      setTimeout(() => {
        if (notification.parentNode) {
          notification.remove();
        }
      }, 300); // Wait for fade-out animation
    }, 3000);
  }

  // --- Context menu for message table ---
  let contextMenu = null;
  
  function showContextMenu(x, y, messageValue) {
    // Remove existing context menu if any
    if (contextMenu) {
      contextMenu.remove();
    }
    
    // Create context menu
    contextMenu = document.createElement('div');
    contextMenu.className = 'context-menu';
    contextMenu.style.left = x + 'px';
    contextMenu.style.top = y + 'px';
    contextMenu.innerHTML = `
      <div class="context-menu-item" data-action="copy-value">Копировать значение</div>
    `;
    
    document.body.appendChild(contextMenu);
    
    // Handle menu item click
    const copyItem = contextMenu.querySelector('[data-action="copy-value"]');
    copyItem.addEventListener('click', () => {
      copyToClipboard(messageValue);
      contextMenu.remove();
      contextMenu = null;
    });
    
    // Close menu on click outside
    const closeMenu = (e) => {
      if (contextMenu && !contextMenu.contains(e.target)) {
        contextMenu.remove();
        contextMenu = null;
        document.removeEventListener('click', closeMenu);
      }
    };
    
    setTimeout(() => {
      document.addEventListener('click', closeMenu);
    }, 0);
  }
  
  function copyToClipboard(text) {
    // Use modern Clipboard API if available
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(() => {
        // Show feedback (optional)
        console.log('Значение скопировано в буфер обмена');
      }).catch(err => {
        console.error('Ошибка при копировании:', err);
        fallbackCopyToClipboard(text);
      });
    } else {
      fallbackCopyToClipboard(text);
    }
  }
  
  function fallbackCopyToClipboard(text) {
    // Fallback for older browsers
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    textArea.style.top = '-999999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    try {
      document.execCommand('copy');
      console.log('Значение скопировано в буфер обмена');
    } catch (err) {
      console.error('Ошибка при копировании:', err);
    }
    document.body.removeChild(textArea);
  }

  // --- Message view ---
  function renderMessageView(container, tab) {
    const topicName = tab.topic || tab.title.replace('Сообщения: ', '');
    const brokerId = tab.brokerId;
    let brokerName = tab.brokerName || '';
    
    if (!brokerId && brokerId !== 0) {
      console.error('BrokerId is required for loading messages. Tab data:', tab);
      container.innerHTML = '<div class="alert alert-danger">Ошибка: Не указан ID брокера для загрузки сообщений. Проверьте консоль для деталей.</div>';
      return;
    }
    
    container.innerHTML = '';
    container.appendChild(document.querySelector('[data-tab-content="message-view"]').cloneNode(true));

    const meta = container.querySelector('#message-meta');
    const filterGrid = container.querySelector('.message-filter-grid');
    const tbody = container.querySelector('#message-table tbody');
    const searchTypeEl = container.querySelector('#msg-search-type');
    const limitEl = container.querySelector('#msg-limit');
    const fromEl = container.querySelector('#msg-from');
    const toEl = container.querySelector('#msg-to');
    const filterEl = container.querySelector('#msg-filter');
    const searchBtn = container.querySelector('#msg-search-btn');
    const createBtn = container.querySelector('#msg-create-btn');
    const consumersBtn = container.querySelector('#msg-consumers-btn');

    // Store broker name and partitions info for reuse - scoped to this tab
    let cachedBrokerName = brokerName;
    let cachedPartitionsInfo = null;
    
    // Function to update meta info display - ensure we're updating the correct tab's meta
    const updateMetaInfo = (showLoading = false, partitionsInfoToUse = null) => {
      // Verify we're still on the correct tab and topic
      const currentMeta = container.querySelector('#message-meta');
      if (!currentMeta) return; // Tab might have been closed
      
      // Use provided partitions info or cached one
      const partitionsInfo = partitionsInfoToUse || cachedPartitionsInfo;
      
      let metaHtml = `<div class="message-meta-content">`;
      metaHtml += `<span class="message-meta-item"><strong>Брокер:</strong> ${cachedBrokerName || 'неизвестно'}</span>`;
      
      if (showLoading || !partitionsInfo) {
        metaHtml += `<span class="message-meta-separator">|</span>`;
        metaHtml += `<span class="message-meta-item"><strong>Сообщений:</strong> <span class="loading-spinner">⏳</span></span>`;
        
        if (partitionsInfo && partitionsInfo.partitions && partitionsInfo.partitions.length > 0) {
          metaHtml += `<span class="message-meta-separator">|</span>`;
          metaHtml += `<span class="message-meta-item"><strong>Партиции:</strong></span>`;
          metaHtml += `<div class="partitions-table-wrapper">`;
          metaHtml += `<table class="table table-dark table-sm partitions-table">`;
          metaHtml += `<thead><tr><th>П</th><th>Мин</th><th>Макс</th></tr></thead>`;
          metaHtml += `<tbody>`;
          partitionsInfo.partitions.forEach(p => {
            metaHtml += `<tr><td>${p.partitionId}</td><td>${p.minOffset.toLocaleString()}</td><td>${p.maxOffset.toLocaleString()}</td></tr>`;
          });
          metaHtml += `</tbody></table>`;
          metaHtml += `</div>`;
        } else {
          metaHtml += `<span class="message-meta-separator">|</span>`;
          metaHtml += `<span class="message-meta-item"><strong>Партиции:</strong> <span class="loading-spinner">⏳</span></span>`;
        }
      } else {
        const displayCount = partitionsInfo.totalMessages || 0;
        metaHtml += `<span class="message-meta-separator">|</span>`;
        metaHtml += `<span class="message-meta-item"><strong>Сообщений:</strong> ${displayCount.toLocaleString()}</span>`;
        
        if (partitionsInfo.partitions && partitionsInfo.partitions.length > 0) {
          metaHtml += `<span class="message-meta-separator">|</span>`;
          metaHtml += `<span class="message-meta-item"><strong>Партиции:</strong></span>`;
          metaHtml += `<div class="partitions-table-wrapper">`;
          metaHtml += `<table class="table table-dark table-sm partitions-table">`;
          metaHtml += `<thead><tr><th>П</th><th>Мин</th><th>Макс</th></tr></thead>`;
          metaHtml += `<tbody>`;
          partitionsInfo.partitions.forEach(p => {
            metaHtml += `<tr><td>${p.partitionId}</td><td>${p.minOffset.toLocaleString()}</td><td>${p.maxOffset.toLocaleString()}</td></tr>`;
          });
          metaHtml += `</tbody></table>`;
          metaHtml += `</div>`;
        }
      }
      
      metaHtml += `</div>`;
      currentMeta.innerHTML = metaHtml;
    };
    
    // Load and display broker info and partition info - ensure we use correct topic and broker
    const loadBrokerAndPartitionInfo = async (currentTopicName = topicName, currentBrokerId = brokerId) => {
      // Verify we're still on the correct tab
      const currentMeta = container.querySelector('#message-meta');
      if (!currentMeta) return; // Tab might have been closed
      
      let brokerNameToDisplay = brokerName;
      
      // Fetch broker name if not provided
      if (!brokerNameToDisplay) {
        try {
          const brokersResponse = await fetch('/api/brokers');
          const brokers = await brokersResponse.json();
          const broker = brokers.find(b => (b.id === currentBrokerId) || (b.Id === currentBrokerId));
          if (broker) {
            brokerNameToDisplay = broker.connectionName || broker.ConnectionName || '';
          }
        } catch (e) {
          console.error('Error loading broker info:', e);
        }
      }
      
      cachedBrokerName = brokerNameToDisplay;
      
      // Fetch partition info for the specific topic
      let partitionsInfo = null;
      try {
        const partitionsResponse = await fetch(`/api/topics/${encodeURIComponent(currentTopicName)}/partitions?brokerId=${currentBrokerId}`);
        if (partitionsResponse.ok) {
          partitionsInfo = await partitionsResponse.json();
          // Verify this is still the correct topic before caching
          if (partitionsInfo.topicName === currentTopicName && partitionsInfo.brokerId === currentBrokerId) {
            cachedPartitionsInfo = partitionsInfo;
          }
        }
      } catch (e) {
        console.error('Error loading partition info:', e);
      }
      
      // Update meta info with the loaded partitions info
      updateMetaInfo(false, partitionsInfo || cachedPartitionsInfo);
    };
    
    // Load broker and partition info
    loadBrokerAndPartitionInfo();

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
      if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">Нет сообщений</td></tr>';
        return;
      }
      rows.forEach(m => {
        const tr = document.createElement('tr');
        // Parse UTC timestamp and convert to local time
        const utcDate = new Date(m.timestampUtc);
        // Format UTC time
        const utcTimeString = utcDate.toLocaleString('ru-RU', { timeZone: 'UTC', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
        // Format local time (current region)
        const localTimeString = utcDate.toLocaleString('ru-RU', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
        
        // Create all cells
        const partitionCell = document.createElement('td');
        partitionCell.textContent = m.partition;
        
        const offsetCell = document.createElement('td');
        offsetCell.textContent = m.offset;
        
        const keyCell = document.createElement('td');
        keyCell.textContent = m.key;
        
        // Create value cell with expand/collapse functionality
        const valueCell = document.createElement('td');
        const valueText = String(m.value || '');
        const valueSpan = document.createElement('span');
        valueSpan.className = 'value-content';
        valueSpan.textContent = valueText;
        valueCell.appendChild(valueSpan);
        
        // Check if value needs truncation (rough estimate: more than ~80 chars or contains newlines)
        const needsTruncation = valueText.length > 80 || valueText.includes('\n');
        if (needsTruncation) {
          const expandButton = document.createElement('button');
          expandButton.className = 'expand-button';
          expandButton.textContent = 'показать';
          expandButton.type = 'button';
          expandButton.addEventListener('click', (e) => {
            e.stopPropagation(); // Prevent row click
            const isExpanded = valueCell.classList.contains('expanded');
            if (isExpanded) {
              valueCell.classList.remove('expanded');
              expandButton.textContent = 'показать';
            } else {
              valueCell.classList.add('expanded');
              expandButton.textContent = 'свернуть';
            }
          });
          valueCell.appendChild(expandButton);
        }
        
        const utcTimeCell = document.createElement('td');
        utcTimeCell.textContent = utcTimeString;
        
        const localTimeCell = document.createElement('td');
        localTimeCell.textContent = localTimeString;
        
        // Store original message value in data attribute for context menu
        tr.dataset.messageValue = valueText;
        
        // Append all cells to row
        tr.appendChild(partitionCell);
        tr.appendChild(offsetCell);
        tr.appendChild(keyCell);
        tr.appendChild(valueCell);
        tr.appendChild(utcTimeCell);
        tr.appendChild(localTimeCell);
        
        tr.addEventListener('click', () => {
          // Remove active class from all rows
          tbody.querySelectorAll('tr').forEach(row => row.classList.remove('active'));
          // Add active class to clicked row
          tr.classList.add('active');
        });
        
        // Context menu for copying value
        tr.addEventListener('contextmenu', (e) => {
          e.preventDefault();
          showContextMenu(e.pageX, e.pageY, valueText);
        });
        
        tbody.appendChild(tr);
      });
    };

    const loadMessages = () => {
      if (!brokerId && brokerId !== 0) {
        console.error('BrokerId is required. Topic:', topicName, 'Tab:', tab);
        container.innerHTML = '<div class="alert alert-danger">Ошибка: Не указан ID брокера для загрузки сообщений</div>';
        return;
      }
      
      console.log('Loading messages for topic:', topicName, 'brokerId:', brokerId);
      
      // Show loading state in table and meta info
      tbody.innerHTML = '<tr><td colspan="6" style="text-align: center;">Загрузка...</td></tr>';
      updateMetaInfo(true); // Show loading spinner in meta info
      
      const params = new URLSearchParams();
      params.append('topic', topicName);
      params.append('brokerId', brokerId.toString());
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

      const url = '/api/messages?' + params.toString();
      console.log('Fetching messages from:', url);

      fetch(url)
        .then(r => {
          console.log('Response status:', r.status);
          if (!r.ok) {
            return r.text().then(text => {
              throw new Error(`HTTP ${r.status}: ${text}`);
            });
          }
          return r.json();
        })
        .then(data => {
          console.log('Received messages:', data);
          const messages = data ?? [];
          renderRows(messages);
          
          // Reload partition info for THIS specific topic to get updated counts
          // Pass current topicName and brokerId explicitly to ensure correct topic
          loadBrokerAndPartitionInfo(topicName, brokerId);
        })
        .catch(error => {
          console.error('Error loading messages:', error);
          const fallback = [{
            partition: 0,
            offset: 0,
            key: 'н/д',
            value: `Ошибка загрузки: ${error.message}`,
            timestampUtc: new Date().toISOString()
          }];
          renderRows(fallback);
          updateMetaInfo(false); // Remove loading spinner on error
        });
    };

    searchBtn.addEventListener('click', loadMessages);
    filterEl.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') loadMessages();
    });

    // Create message modal - use single global modal to avoid duplicates
    const modalId = 'create-message-modal';
    let createModal = document.getElementById(modalId);
    let messageEditor = null;
    
    // Clean up any existing CodeMirror instances in the modal
    if (createModal) {
      const existingTextarea = createModal.querySelector('#msg-message-value');
      if (existingTextarea) {
        // Check if CodeMirror wrapper exists
        const cmWrapper = existingTextarea.nextElementSibling;
        if (cmWrapper && cmWrapper.classList.contains('CodeMirror')) {
          // Try to get CodeMirror instance and destroy it
          try {
            const cmInstance = existingTextarea.CodeMirror || cmWrapper.CodeMirror;
            if (cmInstance) {
              cmInstance.toTextArea();
            }
          } catch (e) {
            // If we can't get the instance, remove the wrapper manually
            cmWrapper.remove();
          }
        }
      }
    }
    
    if (!createModal) {
      createModal = document.createElement('div');
      createModal.id = modalId;
      createModal.className = 'modal-overlay';
      createModal.style.display = 'none';
      createModal.innerHTML = `
        <div class="modal-content glass-panel" style="max-width: 700px;">
          <div class="modal-header">
            <h3>Создать сообщение</h3>
            <button type="button" class="modal-close" id="create-message-close">×</button>
          </div>
          <div class="modal-body">
            <form id="create-message-form">
              <div class="form-group">
                <label class="form-label" for="msg-key-type">Тип для Ключа</label>
                <select class="form-select" id="msg-key-type" required>
                  <option value="Null" selected>Null</option>
                  <option value="Number">Number</option>
                  <option value="String">String</option>
                </select>
              </div>
              <div class="form-group">
                <label class="form-label" for="msg-key-value">Ключ</label>
                <input class="form-control" id="msg-key-value" type="text" placeholder="Введите ключ..." />
              </div>
              <div class="form-group">
                <label class="form-label" for="msg-message-value">Сообщение (JSON)</label>
                <textarea class="form-control" id="msg-message-value" rows="10" placeholder='{"key": "value"}'></textarea>
              </div>
              <div class="form-group">
                <div class="form-check">
                  <input class="form-check-input" type="checkbox" id="msg-validate-json" checked />
                  <label class="form-check-label" for="msg-validate-json">Проверять валидность JSON</label>
                </div>
              </div>
            </form>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-outline-secondary" id="create-message-cancel">Отмена</button>
            <button type="button" class="btn btn-primary" id="create-message-apply">Создать</button>
          </div>
        </div>
      `;
      document.body.appendChild(createModal);
    }

     // Get form elements - these need to be re-queried after cloning
     let modalTitle = createModal.querySelector('h3');
     let modalClose = createModal.querySelector('#create-message-close');
     let modalCancel = createModal.querySelector('#create-message-cancel');
     let modalApply = createModal.querySelector('#create-message-apply');
     let messageForm = createModal.querySelector('#create-message-form');
     let keyTypeInput = createModal.querySelector('#msg-key-type');
     let keyValueInput = createModal.querySelector('#msg-key-value');
     let messageValueInput = createModal.querySelector('#msg-message-value');
     let validateJsonCheckbox = createModal.querySelector('#msg-validate-json');
     
     // Store current topic name and tab ID in closure for this render
     const currentTopicName = topicName;
     const currentTabId = tab.id || `message-${topicName}`;
     
     // Remove existing event listeners by cloning nodes to remove all handlers
     const newModalClose = modalClose.cloneNode(true);
     const newModalCancel = modalCancel.cloneNode(true);
     const newModalApply = modalApply.cloneNode(true);
     modalClose.parentNode.replaceChild(newModalClose, modalClose);
     modalCancel.parentNode.replaceChild(newModalCancel, modalCancel);
     modalApply.parentNode.replaceChild(newModalApply, modalApply);
     
     // Update references to the new cloned elements
     modalClose = newModalClose;
     modalCancel = newModalCancel;
     modalApply = newModalApply;
     
     const openCreateModal = () => {
      // Destroy existing CodeMirror instance if any
      if (messageEditor) {
        try {
          messageEditor.toTextArea();
        } catch (e) {
          console.warn('Error destroying existing CodeMirror:', e);
        }
        messageEditor = null;
      }
      
      // Also check for any orphaned CodeMirror wrappers
      const existingCmWrapper = messageValueInput.nextElementSibling;
      if (existingCmWrapper && existingCmWrapper.classList.contains('CodeMirror')) {
        existingCmWrapper.remove();
      }
      
       // Reset form values
       keyTypeInput.value = 'Null';
       keyValueInput.value = '';
       messageValueInput.value = '{\n  \n}';
       validateJsonCheckbox.checked = true;
      
      // Show modal first
      createModal.style.display = 'flex';
      
      // Create new CodeMirror instance after modal is visible
      // Use requestAnimationFrame to ensure DOM is ready
      requestAnimationFrame(() => {
        try {
          messageEditor = CodeMirror.fromTextArea(messageValueInput, {
            mode: 'application/json',
            theme: 'dracula',
            lineNumbers: true,
            indentUnit: 2,
            indentWithTabs: false,
            autoCloseBrackets: true,
            matchBrackets: true,
            lineWrapping: true
          });
          messageEditor.setSize('100%', '300px');
          messageEditor.setValue('{\n  \n}');
          
          // Force refresh to ensure proper rendering
          setTimeout(() => {
            if (messageEditor) {
              messageEditor.refresh();
              messageEditor.focus();
            }
          }, 50);
        } catch (e) {
          console.error('Error initializing CodeMirror:', e);
        }
      });
    };

    const closeCreateModal = () => {
      // Destroy CodeMirror instance when closing modal
      if (messageEditor) {
        try {
          messageEditor.toTextArea();
        } catch (e) {
          console.warn('Error destroying CodeMirror on close:', e);
        }
        messageEditor = null;
      }
      
      createModal.style.display = 'none';
      messageForm.reset();
    };

     // Add event listeners to the cloned buttons (old handlers are removed by cloning)
     modalClose.addEventListener('click', closeCreateModal);
     modalCancel.addEventListener('click', closeCreateModal);
     
     // Remove existing click handler on modal overlay by using a flag
     if (!createModal.dataset.clickHandlerAttached) {
       createModal.addEventListener('click', (e) => {
         if (e.target === createModal) {
           closeCreateModal();
         }
       });
       createModal.dataset.clickHandlerAttached = 'true';
     }

     modalApply.addEventListener('click', () => {
       // Only process if modal is visible
       if (createModal.style.display !== 'flex') {
         return;
       }
       
       // Check if this is the active tab
       const activeTab = tabState.tabs.find(t => t.id === tabState.activeId);
       if (!activeTab || activeTab.id !== currentTabId) {
         console.log('Ignoring send from inactive tab:', currentTabId, 'Active:', activeTab?.id);
         return; // Don't send if this is not the active tab
       }
       
       let messageValue = messageEditor ? messageEditor.getValue() : messageValueInput.value;
       
       // Validate JSON only if checkbox is checked
       if (validateJsonCheckbox && validateJsonCheckbox.checked) {
         try {
           if (messageValue.trim()) {
             JSON.parse(messageValue);
           }
         } catch (e) {
           showNotification('Ошибка: Сообщение должно быть валидным JSON', 'error');
           return;
         }
       }

       // Get key value based on type
       let keyValue = null;
       if (keyTypeInput.value === 'Null') {
         keyValue = null;
       } else if (keyTypeInput.value === 'Number') {
         const num = parseFloat(keyValueInput.value);
         if (isNaN(num)) {
           showNotification('Ошибка: Ключ должен быть числом', 'error');
           return;
         }
         keyValue = num;
       } else {
         keyValue = keyValueInput.value.trim() || null;
       }

       const messageData = {
         topic: currentTopicName,
         key: keyValue,
         value: messageValue.trim() || '{}'
       };

       // Send to API
       fetch('/api/messages', {
         method: 'POST',
         headers: {
           'Content-Type': 'application/json'
         },
         body: JSON.stringify(messageData)
       })
         .then(r => {
           if (r.ok) {
             showNotification('Сообщение успешно отправлено', 'success');
             // Only reload messages if this is still the active tab
             if (tabState.activeId === currentTabId) {
               loadMessages(); // Reload messages
             }
           } else {
             return r.text().then(text => {
               throw new Error(text || 'Ошибка при отправке сообщения');
             });
           }
         })
         .catch(error => {
           showNotification('Ошибка при отправке сообщения: ' + error.message, 'error');
         });
     });

    createBtn.addEventListener('click', openCreateModal);

    consumersBtn.addEventListener('click', () => {
      const tabId = `consumers-${topicName}`;
      openTab(tabId, `Консьюмеры: ${topicName}`, { render: renderConsumerView, topic: topicName });
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
        tr.addEventListener('click', () => {
          // Remove active class from all rows
          tbody.querySelectorAll('tr').forEach(row => row.classList.remove('active'));
          // Add active class to clicked row
          tr.classList.add('active');
        });
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
            { group: `${topicName}-grp`, member: 'consumer-1', lag: 12, status: 'Активен' },
            { group: `${topicName}-grp`, member: 'consumer-2', lag: 3, status: 'Активен' },
            { group: `${topicName}-grp`, member: 'consumer-3', lag: 25, status: 'Перебалансировка' }
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
                <label class="form-label" for="broker-connection-name">Название подключения</label>
                <input class="form-control" id="broker-connection-name" type="text" required />
              </div>
              <div class="form-group">
                <label class="form-label" for="broker-host">Хост</label>
                <input class="form-control" id="broker-host" type="text" required />
              </div>
              <div class="form-group">
                <label class="form-label" for="broker-port">Порт</label>
                <input class="form-control" id="broker-port" type="number" min="1" max="65535" required />
              </div>
              <div class="form-group">
                <label class="form-label" for="broker-status">Статус</label>
                <select class="form-select" id="broker-status" required>
                  <option value="Active">Активен</option>
                  <option value="Inactive">Неактивен</option>
                  <option value="Connecting">Подключение</option>
                  <option value="Error">Ошибка</option>
                </select>
              </div>
              <div class="form-group">
                <div class="form-check">
                  <input class="form-check-input" type="checkbox" id="broker-auth-enabled" />
                  <label class="form-check-label" for="broker-auth-enabled">Авторизация</label>
                </div>
              </div>
              <div id="broker-auth-fields" style="display: none;">
                <div class="form-group">
                  <label class="form-label" for="broker-client-id">ClientId</label>
                  <input class="form-control" id="broker-client-id" type="text" />
                </div>
                <div class="form-group">
                  <label class="form-label" for="broker-client-secret">ClientSecret</label>
                  <input class="form-control" id="broker-client-secret" type="password" />
                </div>
                <div class="form-group">
                  <label class="form-label" for="broker-oidc-endpoint">OIDCEndpoint</label>
                  <input class="form-control" id="broker-oidc-endpoint" type="text" />
                </div>
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
    const brokerConnectionNameInput = document.getElementById('broker-connection-name');
    const brokerHostInput = document.getElementById('broker-host');
    const brokerPortInput = document.getElementById('broker-port');
    const brokerStatusInput = document.getElementById('broker-status');
    const brokerAuthEnabled = document.getElementById('broker-auth-enabled');
    const brokerAuthFields = document.getElementById('broker-auth-fields');
    const brokerClientIdInput = document.getElementById('broker-client-id');
    const brokerClientSecretInput = document.getElementById('broker-client-secret');
    const brokerOIDCEndpointInput = document.getElementById('broker-oidc-endpoint');

    // Toggle auth fields visibility
    brokerAuthEnabled.addEventListener('change', () => {
      brokerAuthFields.style.display = brokerAuthEnabled.checked ? 'block' : 'none';
      if (!brokerAuthEnabled.checked) {
        brokerClientIdInput.value = '';
        brokerClientSecretInput.value = '';
        brokerOIDCEndpointInput.value = '';
      }
    });

    let currentBrokers = [];

    const openModal = (broker = null) => {
      if (broker) {
        modalTitle.textContent = 'Изменить брокер';
        brokerIdInput.value = broker.id;
        brokerConnectionNameInput.value = broker.connectionName || '';
        brokerHostInput.value = broker.host;
        brokerPortInput.value = broker.port;
        brokerStatusInput.value = broker.status;
        
        const hasAuth = broker.clientId || broker.clientSecret || broker.oidcEndpoint;
        brokerAuthEnabled.checked = hasAuth;
        brokerAuthFields.style.display = hasAuth ? 'block' : 'none';
        brokerClientIdInput.value = broker.clientId || '';
        brokerClientSecretInput.value = broker.clientSecret || '';
        brokerOIDCEndpointInput.value = broker.oidcEndpoint || '';
      } else {
        modalTitle.textContent = 'Добавить брокер';
        brokerIdInput.value = '';
        brokerConnectionNameInput.value = '';
        brokerHostInput.value = '';
        brokerPortInput.value = '';
        brokerStatusInput.value = 'Active';
        brokerAuthEnabled.checked = false;
        brokerAuthFields.style.display = 'none';
        brokerClientIdInput.value = '';
        brokerClientSecretInput.value = '';
        brokerOIDCEndpointInput.value = '';
      }
      modal.style.display = 'flex';
    };

    const closeModal = () => {
      modal.style.display = 'none';
      brokerForm.reset();
      brokerAuthEnabled.checked = false;
      brokerAuthFields.style.display = 'none';
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
        connectionName: brokerConnectionNameInput.value.trim(),
        host: brokerHostInput.value.trim(),
        port: parseInt(brokerPortInput.value),
        status: brokerStatusInput.value,
        clientId: brokerAuthEnabled.checked ? brokerClientIdInput.value.trim() || null : null,
        clientSecret: brokerAuthEnabled.checked ? brokerClientSecretInput.value.trim() || null : null,
        oidcEndpoint: brokerAuthEnabled.checked ? brokerOIDCEndpointInput.value.trim() || null : null
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

    const translateStatus = (status) => {
      const statusMap = {
        'Active': 'Активен',
        'Inactive': 'Неактивен',
        'Connecting': 'Подключение',
        'Error': 'Ошибка'
      };
      return statusMap[status] || status;
    };

    const renderRows = (rows) => {
      currentBrokers = rows;
      tbody.innerHTML = '';
      rows.forEach(b => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${b.id}</td>
          <td>${b.connectionName || '-'}</td>
          <td>${b.host}</td>
          <td>${b.port}</td>
          <td>${translateStatus(b.status)}</td>
          <td>${b.clientId || '-'}</td>
          <td>
            <button class="btn-icon btn-edit" data-broker-id="${b.id}" title="Изменить">✏️</button>
            <button class="btn-icon btn-delete" data-broker-id="${b.id}" title="Удалить">🗑️</button>
          </td>
        `;
        
        // Add click handler for row selection
        tr.addEventListener('click', (e) => {
          // Don't select row if clicking on action buttons
          if (e.target.closest('.btn-icon')) {
            return;
          }
          // Remove active class from all rows
          tbody.querySelectorAll('tr').forEach(row => row.classList.remove('active'));
          // Add active class to clicked row
          tr.classList.add('active');
        });
        
        tbody.appendChild(tr);
      });

      // Add event listeners for action buttons
      tbody.querySelectorAll('.btn-edit').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation(); // Prevent row selection
          const brokerId = parseInt(e.target.dataset.brokerId);
          const broker = rows.find(b => b.id === brokerId);
          if (broker) {
            openModal(broker);
          }
        });
      });

      tbody.querySelectorAll('.btn-delete').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation(); // Prevent row selection
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
            { id: 1, connectionName: 'Основной брокер', host: 'localhost', port: 9092, status: 'Active', clientId: 'client-1' },
            { id: 2, connectionName: 'Резервный брокер', host: 'localhost', port: 9093, status: 'Active', clientId: null },
            { id: 3, connectionName: 'Тестовый брокер', host: 'localhost', port: 9094, status: 'Inactive', clientId: 'client-3' }
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

