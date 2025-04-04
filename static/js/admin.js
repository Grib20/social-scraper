let currentUser;

// Переменные для хранения информации об аккаунте Telegram в процессе авторизации
let currentTelegramAccountId = null;

// Функция для сохранения админ-ключа
function saveAdminKey(adminKey) {
    if (!adminKey) {
        console.error('Попытка сохранить пустой admin-key');
        return;
    }
    
    try {
    // Сохраняем в localStorage
    localStorage.setItem('adminKey', adminKey);
    
    // Устанавливаем cookie на всякий случай
        // Устанавливаем срок действия на 30 дней
        const expirationDate = new Date();
        expirationDate.setDate(expirationDate.getDate() + 30);
        
        document.cookie = `admin_key=${adminKey}; path=/; expires=${expirationDate.toUTCString()}; SameSite=Strict`;
        
        const maskedKey = adminKey.length > 4 ? 
            adminKey.substring(0, 2) + '*'.repeat(adminKey.length - 4) + adminKey.substring(adminKey.length - 2) : 
            '****';
        console.log('AdminKey сохранен (маскированный):', maskedKey);
        
        // Проверяем, сохранился ли ключ
        const savedKey = localStorage.getItem('adminKey');
        if (savedKey === adminKey) {
            console.log('Проверка: ключ успешно сохранен в localStorage');
        } else {
            console.error('Проверка: ключ НЕ сохранен в localStorage!');
        }
    } catch (err) {
        console.error('Ошибка при сохранении админ-ключа:', err);
    }
}

// Функция для получения админ-ключа
function getAdminKey() {
    console.log('Извлечение админ-ключа...');
    
    // Сначала пробуем получить из localStorage
    let adminKey = localStorage.getItem('adminKey');
    console.log('Ключ из localStorage:', adminKey ? 'найден' : 'не найден');
    
    // Если нет в localStorage, пробуем получить из cookie
    if (!adminKey) {
        console.log('Попытка получить ключ из cookies...');
        const cookies = document.cookie.split(';');
        console.log('Все cookies:', cookies);
        
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.startsWith('admin_key=')) {
                adminKey = cookie.substring('admin_key='.length, cookie.length);
                console.log('Найден ключ в cookie:', adminKey);
                
            // Если нашли в cookie, сохраняем в localStorage
            if (adminKey) {
                    console.log('Сохраняем ключ из cookie в localStorage');
                localStorage.setItem('adminKey', adminKey);
                }
                break;
            }
        }
    }
    
    // Если ключ найден, выводим замаскированную версию для безопасности
    if (adminKey) {
        const maskedKey = adminKey.length > 4 ? 
            adminKey.substring(0, 2) + '*'.repeat(adminKey.length - 4) + adminKey.substring(adminKey.length - 2) : 
            '****';
        console.log('Итоговый ключ (маскированный):', maskedKey);
    } else {
        console.log('Ключ не найден ни в localStorage, ни в cookies');
    }
    
    return adminKey;
}

// Проверка авторизации при загрузке страницы
document.addEventListener('DOMContentLoaded', async () => {
    console.log('Страница загружена');
    
    try {
    // Получаем админ-ключ из localStorage, cookie или URL параметра
    let adminKey = getAdminKey();
    
    // Если админ-ключ есть в URL, получаем и сохраняем его
    const urlParams = new URLSearchParams(window.location.search);
    const urlAdminKey = urlParams.get('admin_key');
    if (urlAdminKey) {
        console.log('Получен ключ из URL:', urlAdminKey);
        adminKey = urlAdminKey;
        saveAdminKey(adminKey);
        
        // Очищаем URL от параметра admin_key для безопасности
        const newUrl = window.location.pathname;
        window.history.replaceState({}, document.title, newUrl);
    }
    
    console.log('Используемый админ-ключ:', adminKey);
    
    if (!adminKey) {
        console.log('Админ-ключ не найден, перенаправление на /login');
        window.location.href = '/login';
        return;
    }

        console.log('Проверка валидности админ-ключа...');
        const response = await fetch('/admin/validate', {
            method: 'POST',
            headers: {
                'X-Admin-Key': adminKey,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({})
        });

        if (!response.ok) {
            console.error('Ошибка валидации ключа:', response.status);
            localStorage.removeItem('adminKey');
            window.location.href = '/login';
            return;
        }

        console.log('Админ-ключ валиден, загрузка пользователей...');
        // Принудительно сохраняем ключ снова после успешной валидации
        saveAdminKey(adminKey);
        
        // Загружаем пользователей
        await displayUsers();
        
        // Восстанавливаем активную вкладку, если она была сохранена
        const activeTab = localStorage.getItem('activeTab') || 'users';
        
        // Проверяем существование функции switchTab
        if (typeof window.switchTab === 'function') {
            window.switchTab(activeTab);
        } else {
            console.log('Функция switchTab не найдена, используем встроенную версию');
            // Ручное переключение
            const tabs = document.querySelectorAll('.tab-content');
            const buttons = document.querySelectorAll('.tab-btn');
            
            tabs.forEach(tab => tab.classList.remove('active'));
            buttons.forEach(btn => btn.classList.remove('active'));
            
            const activeTabElement = document.getElementById(activeTab + 'Tab');
            if (activeTabElement) activeTabElement.classList.add('active');
            
            const activeButton = document.querySelector(`.tab-btn[onclick="switchTab('${activeTab}')"]`);
            if (activeButton) activeButton.classList.add('active');
            
            // Если переключаемся на вкладку статистики, загружаем её
            if (activeTab === 'stats' && typeof displayAccountsStats === 'function') {
                displayAccountsStats();
            }
        }
        
        // Подключаем обработчик для формы добавления пользователя
        const addUserForm = document.getElementById('addUserForm');
        if (addUserForm) {
            addUserForm.addEventListener('submit', registerUser);
        }
    } catch (error) {
        console.error('Ошибка проверки авторизации:', error);
        // НЕ удаляем ключ, чтобы не потерять его при временных ошибках
        // localStorage.removeItem('adminKey');
        // Показываем сообщение об ошибке вместо перенаправления
        alert('Ошибка при проверке авторизации: ' + error.message);
    }
});

// Функция отображения пользователей
async function displayUsers() {
    const usersContainer = document.getElementById('usersContainer');
    usersContainer.innerHTML = `
        <div class="loading">
            <i class="fas fa-spinner fa-spin"></i>
            <p>Загрузка пользователей...</p>
        </div>
    `;
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }
    
    try {
        const response = await fetch('/admin/users', {
            headers: {
                'X-Admin-Key': adminKey
            }
        });
        
        if (!response.ok) {
            throw new Error(`Ошибка HTTP: ${response.status}`);
        }
        
        const users = await response.json();
        
        if (users.length === 0) {
            usersContainer.innerHTML = `
                <div class="no-data">
                    <i class="fas fa-users-slash"></i>
                    <p>Пользователи не найдены</p>
                </div>
            `;
            return;
        }
        
        usersContainer.innerHTML = '';
        
        users.forEach(user => {
            const userCard = document.createElement('div');
            userCard.className = 'user-card';
            
            // Создаем заголовок карточки с именем пользователя и кнопкой удаления
            const userHeader = document.createElement('div');
            userHeader.className = 'user-header';
            
            const userName = document.createElement('h3');
            userName.innerHTML = `<i class="fas fa-user"></i> ${user.username}`;
            
            const deleteButton = document.createElement('button');
            deleteButton.className = 'delete-user-btn';
            deleteButton.innerHTML = `<i class="fas fa-trash-alt"></i>`;
            deleteButton.onclick = () => deleteUser(user.id);
            
            userHeader.appendChild(userName);
            userHeader.appendChild(deleteButton);
            userCard.appendChild(userHeader);
            
            // Создаем секцию с API ключом
            const apiKeySection = document.createElement('div');
            apiKeySection.className = 'api-key-section';
            
            const apiKeyLabel = document.createElement('div');
            apiKeyLabel.className = 'api-key-label';
            apiKeyLabel.textContent = 'API Ключ:';
            
            const apiKeyContainer = document.createElement('div');
            apiKeyContainer.className = 'api-key-container';
            
            const apiKeyCode = document.createElement('code');
            apiKeyCode.textContent = user.api_key;
            
            const apiKeyActions = document.createElement('div');
            apiKeyActions.className = 'api-key-actions';
            
            const copyButton = document.createElement('button');
            copyButton.innerHTML = `<i class="fas fa-copy"></i>`;
            copyButton.onclick = () => copyToClipboard(user.api_key);
            copyButton.title = 'Копировать';
            
            const regenerateButton = document.createElement('button');
            regenerateButton.innerHTML = `<i class="fas fa-sync-alt"></i>`;
            regenerateButton.onclick = () => regenerateApiKey(user.id);
            regenerateButton.title = 'Сгенерировать новый ключ';
            
            apiKeyActions.appendChild(copyButton);
            apiKeyActions.appendChild(regenerateButton);
            
            apiKeyContainer.appendChild(apiKeyCode);
            apiKeyContainer.appendChild(apiKeyActions);
            
            apiKeySection.appendChild(apiKeyLabel);
            apiKeySection.appendChild(apiKeyContainer);
            
            userCard.appendChild(apiKeySection);
            
            // Создаем секцию с аккаунтами Telegram
            const telegramSection = document.createElement('div');
            telegramSection.className = 'accounts-section';
            
            const telegramHeader = document.createElement('div');
            telegramHeader.className = 'accounts-header';
            
            const telegramTitle = document.createElement('h4');
            telegramTitle.innerHTML = `<i class="fab fa-telegram"></i> Telegram аккаунты`;
            
            const addTelegramButton = document.createElement('button');
            addTelegramButton.className = 'add-account-btn';
            addTelegramButton.innerHTML = `<i class="fas fa-plus"></i> Добавить`;
            addTelegramButton.onclick = () => showTelegramModal(user.id);
            
            telegramHeader.appendChild(telegramTitle);
            telegramHeader.appendChild(addTelegramButton);
            
            const telegramList = document.createElement('div');
            telegramList.className = 'accounts-list';
            
            if (user.telegram_accounts && user.telegram_accounts.length > 0) {
                user.telegram_accounts.forEach(account => {
                    const telegramItem = createAccountItem(
                        'telegram', 
                        account.id, 
                        user.id, 
                        account.phone, 
                        account.status, 
                        account.is_active,
                        account.proxy
                    );
                    telegramList.appendChild(telegramItem);
                });
            } else {
                telegramList.innerHTML = `
                    <div class="no-accounts">
                        <p>Нет добавленных аккаунтов Telegram</p>
                    </div>
                `;
            }
            
            telegramSection.appendChild(telegramHeader);
            telegramSection.appendChild(telegramList);
            
            userCard.appendChild(telegramSection);
            
            // Создаем секцию с аккаунтами VK
            const vkSection = document.createElement('div');
            vkSection.className = 'accounts-section';
            
            const vkHeader = document.createElement('div');
            vkHeader.className = 'accounts-header';
            
            const vkTitle = document.createElement('h4');
            vkTitle.innerHTML = `<i class="fab fa-vk"></i> VK аккаунты`;
            
            const addVkButton = document.createElement('button');
            addVkButton.className = 'add-account-btn';
            addVkButton.innerHTML = `<i class="fas fa-plus"></i> Добавить`;
            addVkButton.onclick = () => showVkModal(user.id);
            
            vkHeader.appendChild(vkTitle);
            vkHeader.appendChild(addVkButton);
            
            const vkList = document.createElement('div');
            vkList.className = 'accounts-list';
            
            if (user.vk_accounts && user.vk_accounts.length > 0) {
                user.vk_accounts.forEach(account => {
                    let displayName = account.user_name || "Токен: " + maskToken(account.token);
                    const vkItem = createAccountItem(
                        'vk', 
                        account.id, 
                        user.id, 
                        displayName, 
                        account.status, 
                        account.is_active,
                        account.proxy
                    );
                    vkList.appendChild(vkItem);
                });
            } else {
                vkList.innerHTML = `
                    <div class="no-accounts">
                        <p>Нет добавленных аккаунтов VK</p>
                    </div>
                `;
            }
            
            vkSection.appendChild(vkHeader);
            vkSection.appendChild(vkList);
            
            userCard.appendChild(vkSection);
            
            usersContainer.appendChild(userCard);
        });
    } catch (error) {
        console.error('Ошибка при загрузке пользователей:', error);
        usersContainer.innerHTML = `
            <div class="error-message">
                <i class="fas fa-exclamation-triangle"></i>
                <p>Ошибка при загрузке пользователей: ${error.message}</p>
            </div>
        `;
    }
}

// Функция для маскирования токена (показывает только первые и последние 4 символа)
function maskToken(token) {
    if (!token) return 'Нет токена';
    if (token.length <= 8) return token;
    
    return token.substring(0, 4) + '...' + token.substring(token.length - 4);
}

// Функция создания элемента аккаунта с информацией о прокси
function createAccountItem(platform, accountId, userId, displayName, status, lastActivity, proxy = null) {
    const accountIcon = platform === 'telegram' ? 'fab fa-telegram' : 'fab fa-vk';
    const statusClass = status === 'active' || status === 'Активен' ? 'active' : 'inactive';
    const statusText = getStatusText(status);
    
    // Форматирование даты последнего использования
    let lastUsedText = "Не использовался";
    let lastUsedClass = "not-used";
    
    if (lastActivity && lastActivity !== "Invalid Date") {
        try {
            const date = new Date(lastActivity);
            if (!isNaN(date.getTime())) {
                lastUsedText = date.toLocaleString();
                lastUsedClass = "";
            }
        } catch (e) {
            console.error("Ошибка форматирования даты:", e);
        }
    }
    
    // Создаем DOM элемент вместо HTML строки
    const accountItem = document.createElement('div');
    accountItem.className = 'account-item';
    accountItem.setAttribute('data-id', accountId);
    accountItem.setAttribute('data-platform', platform);
    
    // Информация об аккаунте
    const accountInfo = document.createElement('div');
    accountInfo.className = 'account-info';
    
    // Имя аккаунта
    const accountName = document.createElement('span');
    accountName.className = 'account-name';
    
    const icon = document.createElement('i');
    icon.className = accountIcon;
    accountName.appendChild(icon);
    
    // Добавляем имя и пробел
    accountName.append(' ', displayName);
    
    // Индикатор статуса
    const statusIndicator = document.createElement('span');
    statusIndicator.className = `status-indicator ${statusClass}`;
    statusIndicator.setAttribute('data-tooltip', `Статус: ${statusText}`);
    accountName.appendChild(statusIndicator);
    
    accountInfo.appendChild(accountName);
    
    // Детали аккаунта
    const accountDetails = document.createElement('div');
    accountDetails.className = 'account-details';
    
    // ID аккаунта
    const accountIdElem = document.createElement('span');
    accountIdElem.className = 'account-id';
    accountIdElem.textContent = `ID: ${platform}-${accountId.substring(0, 8)}`;
    accountDetails.appendChild(accountIdElem);
    
    // Последнее использование
    const lastUsage = document.createElement('div');
    lastUsage.className = 'last-usage';
    
    const lastUsageLabel = document.createElement('span');
    lastUsageLabel.className = 'last-usage-label';
    lastUsageLabel.textContent = 'Последнее использование:';
    
    const lastUsageValue = document.createElement('span');
    lastUsageValue.className = `last-usage-value ${lastUsedClass}`;
    lastUsageValue.textContent = lastUsedText;
    
    lastUsage.appendChild(lastUsageLabel);
    lastUsage.appendChild(lastUsageValue);
    accountDetails.appendChild(lastUsage);
    
    // Прокси
    const proxyInfo = document.createElement('div');
    proxyInfo.className = 'proxy-info';
    
    const proxyLabel = document.createElement('span');
    proxyLabel.className = 'proxy-label';
    proxyLabel.textContent = 'Прокси:';
    
    const proxyValue = document.createElement('span');
    proxyValue.className = 'proxy-value';
    proxyValue.textContent = proxy ? maskProxy(proxy) : 'Не установлен';
    
    proxyInfo.appendChild(proxyLabel);
    proxyInfo.appendChild(proxyValue);
    accountDetails.appendChild(proxyInfo);
    
    accountInfo.appendChild(accountDetails);
    accountItem.appendChild(accountInfo);
    
    // Действия
    const accountActions = document.createElement('div');
    accountActions.className = 'account-actions';
    
    // Кнопка проверки статуса
    const checkBtn = document.createElement('button');
    checkBtn.className = 'action-btn check-btn';
    checkBtn.onclick = function() { checkAccountStatus(platform, accountId); };
    const checkIcon = document.createElement('i');
    checkIcon.className = 'fas fa-sync-alt';
    checkBtn.appendChild(checkIcon);
    
    // Кнопка проверки прокси
    const proxyBtn = document.createElement('button');
    proxyBtn.className = 'action-btn proxy-btn';
    proxyBtn.onclick = function() { checkProxyValidity(platform, accountId); };
    const proxyIcon = document.createElement('i');
    proxyIcon.className = 'fas fa-network-wired';
    proxyBtn.appendChild(proxyIcon);
    
    // Кнопка редактирования прокси
    const proxyEditBtn = document.createElement('button');
    proxyEditBtn.className = 'action-btn proxy-edit-btn';
    proxyEditBtn.onclick = function() { showChangeProxyModal(platform, accountId, userId); };
    const proxyEditIcon = document.createElement('i');
    proxyEditIcon.className = 'fas fa-edit';
    proxyEditBtn.appendChild(proxyEditIcon);
    
    // Кнопка удаления
    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'action-btn delete-btn';
    deleteBtn.onclick = function() { deleteAccount(platform, accountId, userId); };
    const deleteIcon = document.createElement('i');
    deleteIcon.className = 'fas fa-trash';
    deleteBtn.appendChild(deleteIcon);
    
    accountActions.appendChild(checkBtn);
    accountActions.appendChild(proxyBtn);
    accountActions.appendChild(proxyEditBtn);
    accountActions.appendChild(deleteBtn);
    
    accountItem.appendChild(accountActions);
    
    return accountItem;
}

// Функция для маскировки прокси (безопасность)
function maskProxy(proxy) {
    if (!proxy) return 'Не установлен';
    
    try {
        // Разбираем URL прокси
        let url = proxy;
        let protocol = '';
        
        if (proxy.includes('://')) {
            const parts = proxy.split('://');
            protocol = parts[0] + '://';
            url = parts[1];
        }
        
        // Если есть логин и пароль
        if (url.includes('@')) {
            const [auth, hostPort] = url.split('@');
            // Возвращаем звездочки вместо логина и пароля
            return `${protocol}***@${hostPort}`;
        }
        
        return proxy;
    } catch (error) {
        console.error('Ошибка при маскировке прокси:', error);
        return proxy;
    }
}

// Функция для проверки валидности прокси
async function checkProxyValidity(platform, accountId) {
    try {
        showNotification('Проверка прокси...', 'info');
        
        const response = await fetch('/api/admin/check-proxy', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Admin-Key': getAdminKey()
            },
            body: JSON.stringify({
                platform,
                account_id: accountId
            })
        });
        
        const result = await response.json();
        
        if (result.valid) {
            showNotification(result.message, 'success');
        } else {
            showNotification(result.message, 'error');
        }
    } catch (error) {
        console.error('Ошибка при проверке прокси:', error);
        showNotification('Ошибка при проверке прокси: ' + error.message, 'error');
    }
}

// Показать модальное окно для изменения прокси
function showChangeProxyModal(platform, accountId, userId) {
    // Создаем модальное окно, если его еще нет
    let proxyModal = document.getElementById('proxyChangeModal');
    
    if (!proxyModal) {
        proxyModal = document.createElement('div');
        proxyModal.id = 'proxyChangeModal';
        proxyModal.className = 'modal';
        
        proxyModal.innerHTML = `
            <div class="modal-content">
                <span class="close" onclick="closeProxyModal()">&times;</span>
                <h3><i class="fas fa-network-wired"></i> Изменение прокси</h3>
                <form id="changeProxyForm">
                    <input type="hidden" id="proxy_platform" name="platform">
                    <input type="hidden" id="proxy_account_id" name="account_id">
                    <input type="hidden" id="proxy_user_id" name="user_id">
                    
                    <div class="form-group">
                        <label for="proxy_value"><i class="fas fa-network-wired"></i> Прокси:</label>
                        <input type="text" id="proxy_value" name="proxy" placeholder="socks5://user:pass@host:port">
                        <small>Укажите прокси в формате socks5://user:pass@host:port или http://user:pass@host:port</small>
                    </div>
                    
                    <div class="form-actions">
                        <button type="submit" class="save-button"><i class="fas fa-save"></i> Сохранить</button>
                        <button type="button" class="remove-button" onclick="removeProxy()"><i class="fas fa-trash"></i> Удалить прокси</button>
                    </div>
                </form>
            </div>
        `;
        
        document.body.appendChild(proxyModal);
        
        // Обработчик формы
        document.getElementById('changeProxyForm').addEventListener('submit', function(event) {
            event.preventDefault();
            updateProxy();
        });
    }
    
    // Заполняем данные формы
    document.getElementById('proxy_platform').value = platform;
    document.getElementById('proxy_account_id').value = accountId;
    document.getElementById('proxy_user_id').value = userId;
    
    // Показываем модальное окно
    proxyModal.style.display = 'block';
}

// Закрыть модальное окно прокси
function closeProxyModal() {
    const modal = document.getElementById('proxyChangeModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

// Обновить прокси
async function updateProxy() {
    try {
        const platform = document.getElementById('proxy_platform').value;
        const accountId = document.getElementById('proxy_account_id').value;
        const userId = document.getElementById('proxy_user_id').value;
        const proxy = document.getElementById('proxy_value').value;
        
        const response = await fetch('/api/admin/update-proxy', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Admin-Key': getAdminKey()
            },
            body: JSON.stringify({
                platform,
                account_id: accountId,
                user_id: userId,
                proxy
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showNotification(result.message, 'success');
            closeProxyModal();
            // Обновляем данные на странице
            displayAccounts();
        } else {
            showNotification(result.detail || 'Ошибка при обновлении прокси', 'error');
        }
    } catch (error) {
        console.error('Ошибка при обновлении прокси:', error);
        showNotification('Ошибка при обновлении прокси: ' + error.message, 'error');
    }
}

// Удалить прокси
async function removeProxy() {
    try {
        const platform = document.getElementById('proxy_platform').value;
        const accountId = document.getElementById('proxy_account_id').value;
        const userId = document.getElementById('proxy_user_id').value;
        
        const response = await fetch('/api/admin/update-proxy', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Admin-Key': getAdminKey()
            },
            body: JSON.stringify({
                platform,
                account_id: accountId,
                user_id: userId,
                proxy: null
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showNotification(result.message, 'success');
            closeProxyModal();
            // Обновляем данные на странице
            displayAccounts();
        } else {
            showNotification(result.detail || 'Ошибка при удалении прокси', 'error');
        }
    } catch (error) {
        console.error('Ошибка при удалении прокси:', error);
        showNotification('Ошибка при удалении прокси: ' + error.message, 'error');
    }
}

// Функция для получения текстового представления статуса
function getStatusText(status) {
    const statusMap = {
        'active': 'Активен',
        'inactive': 'Неактивен',
        'error': 'Ошибка',
        'pending': 'В обработке',
        'rate_limited': 'Ограничен',
        'banned': 'Заблокирован',
        'validation_required': 'Требуется валидация',
        'unknown': 'Неизвестно'
    };
    
    return statusMap[status.toLowerCase()] || status;
}

// Копировать в буфер обмена
function copyToClipboard(text) {
    if (!text) {
        showNotification('Нет API ключа для копирования', 'error');
        return;
    }
    
    if (navigator.clipboard) {
        navigator.clipboard.writeText(text)
            .then(() => {
                showNotification('API ключ скопирован в буфер обмена', 'success');
            })
            .catch(err => {
                console.error('Ошибка при копировании: ', err);
                showNotification('Ошибка при копировании API ключа', 'error');
            });
    } else {
        // Резервный метод для старых браузеров
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        textArea.style.top = '-999999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        
        try {
            const successful = document.execCommand('copy');
            if (successful) {
                showNotification('API ключ скопирован в буфер обмена', 'success');
            } else {
                showNotification('Не удалось скопировать API ключ', 'error');
            }
        } catch (err) {
            console.error('Ошибка при копировании: ', err);
            showNotification('Ошибка при копировании API ключа', 'error');
        }
        
        document.body.removeChild(textArea);
    }
}

// Функция для отображения уведомлений
function showNotification(message, type = 'info', duration = 4000) {
    // Проверяем, существует ли уже контейнер для уведомлений
    let notificationsContainer = document.querySelector('.notifications');
    
    if (!notificationsContainer) {
        // Создаем контейнер, если его нет
        notificationsContainer = document.createElement('div');
        notificationsContainer.className = 'notifications';
        document.body.appendChild(notificationsContainer);
    }
    
    // Создаем новое уведомление
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    
    // Определяем иконку в зависимости от типа
    let icon = 'info-circle';
    
    if (type === 'success') icon = 'check-circle';
    if (type === 'error') icon = 'exclamation-circle';
    if (type === 'warning') icon = 'exclamation-triangle';
    
    // Содержимое уведомления
    notification.innerHTML = `
        <div class="notification-content">
            <i class="fas fa-${icon}"></i>
            <span>${message}</span>
        </div>
        <button class="close-notification">
            <i class="fas fa-times"></i>
        </button>
    `;
    
    // Добавляем уведомление в контейнер
    notificationsContainer.appendChild(notification);
    
    // Показываем уведомление с анимацией
    setTimeout(() => {
        notification.classList.add('show');
    }, 10);
    
    // Настраиваем обработчик для закрытия уведомления
    const closeButton = notification.querySelector('.close-notification');
    if (closeButton) {
        closeButton.addEventListener('click', () => {
            closeNotification(notification);
        });
    }
    
    // Автоматически закрываем уведомление через указанное время
    if (duration > 0) {
        setTimeout(() => {
            closeNotification(notification);
        }, duration);
    }
    
    return notification;
}

// Функция для закрытия уведомления
function closeNotification(notification) {
    // Удаляем класс show для запуска анимации исчезновения
    notification.classList.remove('show');
    
    // Удаляем элемент после завершения анимации
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 300); // Время равно продолжительности анимации
}

// Функции для работы с модальными окнами
function showRegisterModal() {
    document.getElementById('registerModal').style.display = 'block';
    document.getElementById('username').focus();
}

function showTelegramModal(userId) {
    currentUser = userId;
    
    // Устанавливаем ID пользователя в скрытое поле
    const hiddenField = document.createElement('input');
    hiddenField.type = 'hidden';
    hiddenField.name = 'userId';
    hiddenField.value = userId;
    
    const form = document.getElementById('addTelegramForm');
    if (form) {
        // Удаляем старое поле userId, если оно есть
        const oldField = form.querySelector('input[name="userId"]');
        if (oldField) {
            form.removeChild(oldField);
        }
        form.appendChild(hiddenField);
    }
    
    // Сбрасываем состояние модального окна
    resetTelegramModal();
    
    // Показываем модальное окно
    document.getElementById('telegramModal').style.display = 'block';
    document.getElementById('api_id').focus();
}

function showVkModal(userId) {
    currentUser = userId;
    
    // Устанавливаем ID пользователя в скрытое поле
    const hiddenField = document.createElement('input');
    hiddenField.type = 'hidden';
    hiddenField.name = 'userId';
    hiddenField.value = userId;
    
    const form = document.getElementById('addVkForm');
    if (form) {
        // Удаляем старое поле userId, если оно есть
        const oldField = form.querySelector('input[name="userId"]');
        if (oldField) {
            form.removeChild(oldField);
        }
        form.appendChild(hiddenField);
    }
    
    // Сбрасываем форму
    if (form) {
        form.reset();
    }
    
    // Показываем модальное окно
    document.getElementById('vkModal').style.display = 'block';
    document.getElementById('token').focus();
}

function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.style.display = 'none';
    }
    
    // Сбрасываем состояние Telegram модального окна
    if (modalId === 'telegramModal') {
        resetTelegramModal();
    }
    
    // Сбрасываем состояние VK модального окна
    if (modalId === 'vkModal') {
        resetVkModal();
    }
}

// Функция регистрации нового пользователя
async function registerUser(e) {
    e.preventDefault();
    const formData = new FormData(e.target);
    const data = {
        username: formData.get('username'),
        password: formData.get('password')
    };
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }
    
    // Показываем индикатор загрузки
    const submitButton = e.target.querySelector('button[type="submit"]');
    const originalButtonText = submitButton.innerHTML;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Создание...';
    
    try {
        const response = await fetch('/register', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Admin-Key': adminKey
            },
            body: JSON.stringify(data)
        });
        
        // Восстанавливаем кнопку
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
        
        if (response.ok) {
            const result = await response.json();
            
            // Создаем модальное окно для отображения API ключа
            const apiKeyModal = document.createElement('div');
            apiKeyModal.className = 'modal';
            apiKeyModal.id = 'apiKeyModal';
            apiKeyModal.innerHTML = `
                <div class="modal-content">
                    <span class="close" onclick="document.getElementById('apiKeyModal').remove()">&times;</span>
                    <h2><i class="fas fa-check-circle"></i> Пользователь создан</h2>
                    <p>Пользователь <strong>${result.username}</strong> успешно зарегистрирован!</p>
                    <div class="api-key-display">
                        <p>API ключ:</p>
                        <div class="api-key-value">
                            <code>${result.api_key}</code>
                            <button onclick="copyToClipboard('${result.api_key}')">
                                <i class="fas fa-copy"></i> Копировать
                            </button>
                        </div>
                    </div>
                    <p class="api-key-warning">Сохраните этот ключ! Он потребуется для доступа к API.</p>
                    <div class="modal-actions">
                        <button onclick="document.getElementById('apiKeyModal').remove()">Закрыть</button>
                    </div>
                </div>
            `;
            document.body.appendChild(apiKeyModal);
            apiKeyModal.style.display = 'block';
            
            // Очищаем форму и закрываем модальное окно регистрации
            e.target.reset();
            closeModal('registerModal');
            
            // Обновляем список пользователей
            await displayUsers();
        } else {
            const error = await response.json();
            alert(`Ошибка при регистрации пользователя: ${error.detail}`);
        }
    } catch (error) {
        console.error('Ошибка при регистрации:', error);
        alert('Произошла ошибка при регистрации пользователя');
        
        // Восстанавливаем кнопку в случае ошибки
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
    }
}

// Обработчики форм
document.getElementById('addTelegramForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    // Показываем индикатор загрузки
    const submitButton = e.target.querySelector('button[type="submit"]');
    const originalButtonText = submitButton.innerHTML;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Добавление...';

    try {
        // Отправляем данные формы как FormData для поддержки загрузки файлов
        const url = `/api/telegram/accounts`;
        const headers = {
            'Authorization': `Bearer ${adminKey}`,
            'X-User-Id': currentUser
        };
        
        const response = await fetch(url, {
            method: 'POST',
            headers: headers,
            body: formData
        });

        if (response.ok) {
            const result = await response.json();
            
            // Восстанавливаем кнопку
            submitButton.disabled = false;
            submitButton.innerHTML = originalButtonText;
            
            if (result.requires_auth) {
                // Если требуется авторизация, показываем блок ввода кода
                currentTelegramAccountId = result.account_id;
                document.getElementById('addTelegramForm').style.display = 'none';
                document.getElementById('telegramAuthBlock').style.display = 'block';
                document.getElementById('authStatus').textContent = 'Ожидание ввода кода...';
            } else {
                // Если авторизация не требуется (уже загружен файл сессии)
                showNotification('Telegram аккаунт успешно добавлен', 'success');
                closeModal('telegramModal');
                await displayUsers();
            }
        } else {
            // Обработка ошибки
            const errorData = await response.json();
            showNotification(`Ошибка: ${errorData.detail || 'Не удалось добавить аккаунт'}`, 'error');
            
            // Восстанавливаем кнопку
            submitButton.disabled = false;
            submitButton.innerHTML = originalButtonText;
        }
    } catch (error) {
        console.error('Ошибка при добавлении Telegram аккаунта:', error);
        showNotification('Произошла ошибка при добавлении аккаунта', 'error');
        
        // Восстанавливаем кнопку
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
    }
});

// Обработчик кнопки подтверждения кода авторизации
document.getElementById('submitAuthCode').addEventListener('click', async () => {
    const authCode = document.getElementById('auth_code').value.trim();
    if (!authCode) {
        showNotification('Введите код авторизации', 'error');
        return;
    }
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }
    
    // Показываем статус
    const statusElement = document.getElementById('authStatus');
    statusElement.textContent = 'Проверка кода...';
    
    // Отключаем кнопку
    const submitButton = document.getElementById('submitAuthCode');
    const originalButtonText = submitButton.innerHTML;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Проверка...';
    
    try {
        const response = await fetch('/api/telegram/verify-code', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${adminKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                account_id: currentTelegramAccountId,
                code: authCode
            })
        });
        
        // Восстанавливаем кнопку
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
        
        if (response.ok) {
            const result = await response.json();
            
            if (result.requires_2fa) {
                // Если требуется 2FA
                document.getElementById('telegramAuthBlock').style.display = 'none';
                document.getElementById('telegram2FABlock').style.display = 'block';
                document.getElementById('twoFAStatus').textContent = 'Введите пароль двухфакторной аутентификации';
            } else {
                // Успешная авторизация без 2FA
                showNotification('Telegram аккаунт успешно авторизован', 'success');
                closeModal('telegramModal');
                resetTelegramModal();
                await displayUsers();
            }
        } else {
            // Ошибка при проверке кода
            const error = await response.json();
            statusElement.textContent = `Ошибка: ${error.detail || 'Неверный код авторизации'}`;
            showNotification(`Ошибка: ${error.detail || 'Неверный код авторизации'}`, 'error');
        }
    } catch (error) {
        console.error('Ошибка при проверке кода авторизации:', error);
        statusElement.textContent = 'Произошла ошибка при проверке кода';
        showNotification('Произошла ошибка при проверке кода авторизации', 'error');
        
        // Восстанавливаем кнопку
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
    }
});

// Обработчик кнопки подтверждения пароля 2FA
document.getElementById('submit2FA').addEventListener('click', async () => {
    const twoFAPassword = document.getElementById('two_fa_password').value;
    if (!twoFAPassword) {
        showNotification('Введите пароль 2FA', 'error');
        return;
    }
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }
    
    // Показываем статус
    const statusElement = document.getElementById('twoFAStatus');
    statusElement.textContent = 'Проверка пароля...';
    
    // Отключаем кнопку
    const submitButton = document.getElementById('submit2FA');
    const originalButtonText = submitButton.innerHTML;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Проверка...';
    
    try {
        const response = await fetch('/api/telegram/verify-2fa', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${adminKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                account_id: currentTelegramAccountId,
                password: twoFAPassword
            })
        });
        
        // Восстанавливаем кнопку
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
        
        if (response.ok) {
            // Успешная авторизация с 2FA
            showNotification('Telegram аккаунт успешно авторизован', 'success');
            closeModal('telegramModal');
            resetTelegramModal();
            await displayUsers();
        } else {
            // Ошибка при проверке пароля 2FA
            const error = await response.json();
            statusElement.textContent = `Ошибка: ${error.detail || 'Неверный пароль 2FA'}`;
            showNotification(`Ошибка: ${error.detail || 'Неверный пароль 2FA'}`, 'error');
        }
    } catch (error) {
        console.error('Ошибка при проверке пароля 2FA:', error);
        statusElement.textContent = 'Произошла ошибка при проверке пароля';
        showNotification('Произошла ошибка при проверке пароля 2FA', 'error');
        
        // Восстанавливаем кнопку
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
    }
});

// Функция сброса состояния модального окна Telegram
function resetTelegramModal() {
    // Сбрасываем форму добавления Telegram
    const addTelegramForm = document.getElementById('addTelegramForm');
    if (addTelegramForm) {
        addTelegramForm.reset();
    }
    
    // Сбрасываем форму загрузки сессии
    const uploadSessionForm = document.getElementById('uploadSessionForm');
    if (uploadSessionForm) {
        uploadSessionForm.reset();
    }
    
    // Скрываем блок авторизации
    const authBlock = document.getElementById('telegramAuthBlock');
    if (authBlock) {
        authBlock.style.display = 'none';
    }
    
    // Скрываем блок 2FA
    const twoFABlock = document.getElementById('telegram2FABlock');
    if (twoFABlock) {
        twoFABlock.style.display = 'none';
    }
    
    console.log('Состояние модального окна Telegram сброшено');
}

// Функция сброса VK модального окна
function resetVkModal() {
    console.log('Сброс состояния модального окна VK');
    
    // Сбрасываем форму
    const form = document.getElementById('addVkForm');
    if (form) {
        form.reset();
    }
}

// Функция удаления аккаунтов
async function deleteTelegramAccount(userId, phone) {
    if (!confirm('Вы уверены, что хотите удалить этот аккаунт Telegram?')) {
        return;
    }

    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    try {
        // Используем новый эндпоинт API и передаем ID пользователя и ID аккаунта
        const response = await fetch(`/api/telegram/accounts/${phone}`, {
            method: 'DELETE',
            headers: {
                'Authorization': `Bearer ${adminKey}`,
                'X-User-Id': userId
            }
        });

        if (response.ok) {
            showNotification('Аккаунт Telegram успешно удален', 'success');
            await displayUsers();
        } else {
            const errorData = await response.json();
            showNotification(`Ошибка: ${errorData.detail || 'Не удалось удалить аккаунт Telegram'}`, 'error');
        }
    } catch (error) {
        console.error('Ошибка:', error);
        showNotification('Произошла ошибка при удалении аккаунта', 'error');
    }
}

async function deleteVkAccount(userId, accountId) {
    if (!confirm('Вы уверены, что хотите удалить этот аккаунт VK?')) {
        return;
    }

    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    try {
        const response = await fetch(`/api/vk/accounts/${accountId}`, {
            method: 'DELETE',
            headers: {
                'Authorization': `Bearer ${adminKey}`,
                'X-User-Id': userId
            }
        });

        if (response.ok) {
            showNotification('Аккаунт VK успешно удален', 'success');
            await displayUsers();
        } else {
            const errorData = await response.json();
            showNotification(`Ошибка: ${errorData.detail || 'Не удалось удалить аккаунт VK'}`, 'error');
        }
    } catch (error) {
        console.error('Ошибка:', error);
        showNotification('Произошла ошибка при удалении аккаунта', 'error');
    }
}

// Функция удаления пользователя
async function deleteUser(userId) {
    if (!confirm('Вы действительно хотите удалить этого пользователя? Это действие невозможно отменить.')) {
        return;
    }

    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    try {
        const response = await fetch(`/admin/users/${userId}`, {
            method: 'DELETE',
            headers: {
                'X-Admin-Key': adminKey
            }
        });

        if (response.ok) {
            // Успешное удаление
            const notification = document.createElement('div');
            notification.className = 'copy-notification';
            notification.innerHTML = '<i class="fas fa-check-circle"></i> Пользователь удален';
            document.body.appendChild(notification);
            
            // Удаляем уведомление через 3 секунды
            setTimeout(() => {
                notification.classList.add('fade-out');
                setTimeout(() => {
                    document.body.removeChild(notification);
                }, 500);
            }, 2500);
            
            // Обновляем список пользователей
            await displayUsers();
        } else {
            const error = await response.json();
            alert(`Ошибка при удалении пользователя: ${error.detail || 'Неизвестная ошибка'}`);
        }
    } catch (error) {
        console.error('Ошибка:', error);
        alert('Произошла ошибка при удалении пользователя');
    }
}

// Функция регенерации API ключа
async function regenerateApiKey(userId) {
    if (!confirm('Вы действительно хотите сгенерировать новый API ключ? Старый ключ перестанет работать.')) {
        return;
    }

    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    try {
        const response = await fetch(`/admin/users/${userId}/regenerate-api-key`, {
            method: 'POST',
            headers: {
                'X-Admin-Key': adminKey,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({})
        });

        if (response.ok) {
            const result = await response.json();
            
            // Создаем модальное окно для отображения нового API ключа
            const apiKeyModal = document.createElement('div');
            apiKeyModal.className = 'modal';
            apiKeyModal.id = 'newApiKeyModal';
            apiKeyModal.innerHTML = `
                <div class="modal-content">
                    <span class="close" onclick="document.getElementById('newApiKeyModal').remove()">&times;</span>
                    <h2><i class="fas fa-key"></i> Новый API ключ</h2>
                    <p>API ключ был успешно обновлен!</p>
                    <div class="api-key-display">
                        <p>Новый API ключ:</p>
                        <div class="api-key-value">
                            <code>${result.api_key}</code>
                            <button onclick="copyToClipboard('${result.api_key}')">
                                <i class="fas fa-copy"></i> Копировать
                            </button>
                        </div>
                    </div>
                    <p class="api-key-warning">Сохраните этот ключ! Он потребуется для доступа к API.</p>
                    <div class="modal-actions">
                        <button onclick="document.getElementById('newApiKeyModal').remove()">Закрыть</button>
                    </div>
                </div>
            `;
            document.body.appendChild(apiKeyModal);
            apiKeyModal.style.display = 'block';
            
            // Обновляем список пользователей
            await displayUsers();
        } else {
            const error = await response.json();
            alert(`Ошибка при обновлении API ключа: ${error.detail || 'Неизвестная ошибка'}`);
        }
    } catch (error) {
        console.error('Ошибка:', error);
        alert('Произошла ошибка при обновлении API ключа');
    }
}

// Обработчик формы добавления VK аккаунта
document.getElementById('addVkForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    const data = {
        token: formData.get('token'),
        proxy: formData.get('proxy') || null
    };
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }
    
    // Показываем индикатор загрузки
    const submitButton = e.target.querySelector('button[type="submit"]');
    const originalButtonText = submitButton.innerHTML;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Добавление...';

    try {
        const response = await fetch(`/api/vk/accounts`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${adminKey}`,
                'X-User-Id': currentUser
            },
            body: JSON.stringify(data)
        });
        
        // Восстанавливаем кнопку
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;

        if (response.ok) {
            const result = await response.json();
            showNotification('VK аккаунт успешно добавлен', 'success');
            closeModal('vkModal');
            e.target.reset();
            await displayUsers();
        } else {
            const errorData = await response.json();
            showNotification(`Ошибка: ${errorData.detail || 'Не удалось добавить аккаунт VK'}`, 'error');
        }
    } catch (error) {
        console.error('Ошибка:', error);
        showNotification('Произошла ошибка при добавлении аккаунта VK', 'error');
        
        // Восстанавливаем кнопку
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
    }
}); 

// Функция для проверки статуса аккаунта
async function checkAccountStatus(type, accountId) {
    // Показываем уведомление о начале проверки
    showNotification(`Проверка статуса аккаунта...`, 'info');

    // Получаем админ-ключ
    const adminKey = getAdminKey();
    if (!adminKey) {
        showNotification('Ошибка: Админ-ключ не найден. Пожалуйста, войдите снова.', 'error');
        window.location.href = '/login';
        return;
    }

    let endpoint = '';
    if (type === 'telegram') {
        endpoint = `/api/telegram/accounts/${accountId}/status`;
    } else if (type === 'vk') {
        endpoint = `/api/vk/accounts/${accountId}/status`;
    } else {
        showNotification('Неизвестный тип аккаунта', 'error');
        return;
    }

    try {
        const response = await fetch(endpoint, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${adminKey}`
            }
        });

        const result = await response.json();

        if (response.ok) {
            // Найдем элемент аккаунта по data-id и data-platform
            const accountItem = document.querySelector(`.account-item[data-id="${accountId}"][data-platform="${type}"]`);
            
            if (accountItem) {
                // Находим элемент статуса внутри карточки аккаунта
                const statusElement = accountItem.querySelector('.status-indicator');
                
                if (statusElement) {
                    // Обновляем класс статуса
                    const newStatus = result.status;
                    
                    // Удаляем старые классы статуса
                    statusElement.classList.remove('active', 'inactive', 'cooldown', 'error');
                    
                    // Добавляем новый класс в зависимости от статуса
                    if (newStatus === 'active') {
                        statusElement.classList.add('active');
                    } else if (newStatus === 'inactive') {
                        statusElement.classList.add('inactive');
                    } else if (newStatus === 'cooldown') {
                        statusElement.classList.add('cooldown');
                    } else if (newStatus === 'error') {
                        statusElement.classList.add('error');
                    }
                    
                    // Обновляем подсказку
                    statusElement.setAttribute('data-tooltip', `Статус: ${getStatusLabel(newStatus)}`);
                    
                    showNotification(`Статус аккаунта успешно проверен: ${getStatusLabel(newStatus)}`, 'success');
                } else {
                    console.error('Элемент индикатора статуса не найден в карточке аккаунта');
                    showNotification('Не удалось обновить интерфейс статуса. Обновляем страницу...', 'warning');
                    // Перезагружаем список пользователей
                    await displayUsers();
                }
            } else {
                console.error(`Элемент аккаунта с ID ${accountId} типа ${type} не найден`);
                showNotification('Не удалось найти элемент аккаунта. Обновляем список...', 'warning');
                // Перезагружаем список пользователей
                await displayUsers();
            }
        } else {
            showNotification(`Ошибка проверки статуса: ${result.error || 'Неизвестная ошибка'}`, 'error');
        }
    } catch (error) {
        console.error('Ошибка при проверке статуса:', error);
        showNotification('Ошибка сети при проверке статуса', 'error');
    }
}

/**
 * Отображает статистику аккаунтов
 */
async function displayAccountsStats() {
    console.log('Загрузка статистики аккаунтов...');
    
    // Добавляем анимацию для кнопки обновления
    const refreshBtn = document.querySelector('.refresh-stats-btn');
    if (refreshBtn) {
        const icon = refreshBtn.querySelector('i');
        if (icon) {
            icon.classList.add('fa-spin');
            refreshBtn.disabled = true;
        }
    }
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        console.error('Админ-ключ не найден');
        
        // Останавливаем анимацию
        if (refreshBtn) {
            const icon = refreshBtn.querySelector('i');
            if (icon) {
                icon.classList.remove('fa-spin');
                refreshBtn.disabled = false;
            }
        }
        
        return;
    }

    try {
        const response = await fetch('/api/admin/accounts/stats', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'X-API-KEY': adminKey
            }
        });

        if (!response.ok) {
            throw new Error(`Ошибка HTTP: ${response.status}`);
        }

        const statsData = await response.json();
        console.log('Получены данные статистики:', statsData);
        
        const statsContainer = document.getElementById('accountsStatsContainer');
        if (!statsContainer) {
            console.error('Не найден контейнер для статистики');
            return;
        }
        
        statsContainer.innerHTML = ''; // Очищаем контейнер
        
        // Добавляем подраздел для статистики VK
        if (statsData.vk && statsData.vk.length > 0) {
            const vkSection = document.createElement('div');
            vkSection.className = 'stats-section';
            
            const vkHeader = document.createElement('h2');
            vkHeader.innerHTML = '<i class="fab fa-vk"></i> Статистика VK аккаунтов';
            vkSection.appendChild(vkHeader);
            
            const vkTable = createAccountsStatsTable(statsData.vk, 'vk');
            vkSection.appendChild(vkTable);
            
            statsContainer.appendChild(vkSection);
        } else {
            const noVkData = document.createElement('div');
            noVkData.className = 'stats-section';
            noVkData.innerHTML = '<h2><i class="fab fa-vk"></i> Статистика VK аккаунтов</h2><p>Нет данных о VK аккаунтах</p>';
            statsContainer.appendChild(noVkData);
        }
        
        // Добавляем подраздел для статистики Telegram
        if (statsData.telegram && statsData.telegram.length > 0) {
            const tgSection = document.createElement('div');
            tgSection.className = 'stats-section';
            
            const tgHeader = document.createElement('h2');
            tgHeader.innerHTML = '<i class="fab fa-telegram-plane"></i> Статистика Telegram аккаунтов';
            tgSection.appendChild(tgHeader);
            
            const tgTable = createAccountsStatsTable(statsData.telegram, 'telegram');
            tgSection.appendChild(tgTable);
            
            statsContainer.appendChild(tgSection);
        } else {
            const noTgData = document.createElement('div');
            noTgData.className = 'stats-section';
            noTgData.innerHTML = '<h2><i class="fab fa-telegram-plane"></i> Статистика Telegram аккаунтов</h2><p>Нет данных о Telegram аккаунтах</p>';
            statsContainer.appendChild(noTgData);
        }
        
        // После завершения загрузки
        // Останавливаем анимацию
        if (refreshBtn) {
            const icon = refreshBtn.querySelector('i');
            if (icon) {
                icon.classList.remove('fa-spin');
                refreshBtn.disabled = false;
            }
        }
        
    } catch (error) {
        console.error('Ошибка при загрузке статистики аккаунтов:', error);
        showNotification('Ошибка при загрузке статистики: ' + error.message, 'error');
        
        // Останавливаем анимацию в случае ошибки
        if (refreshBtn) {
            const icon = refreshBtn.querySelector('i');
            if (icon) {
                icon.classList.remove('fa-spin');
                refreshBtn.disabled = false;
            }
        }
    }
}

/**
 * Создает таблицу статистики аккаунтов
 * @param {Array} accounts - массив аккаунтов
 * @param {String} platform - платформа (vk/telegram)
 * @returns {HTMLElement} - таблица статистики
 */
function createAccountsStatsTable(accounts, platform) {
    const table = document.createElement('table');
    table.className = 'stats-table';
    
    // Создаем заголовок таблицы
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    
    const headers = [
        'ID',
        'Логин/Телефон',
        'Лимиты запросов',
        'Использовано',
        '% использования',
        'Последнее использование',
        'Статус'
    ];
    
    headers.forEach(headerText => {
        const th = document.createElement('th');
        th.textContent = headerText;
        headerRow.appendChild(th);
    });
    
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    // Создаем тело таблицы
    const tbody = document.createElement('tbody');
    
    accounts.forEach(account => {
        const row = document.createElement('tr');
        
        // ID
        const idCell = document.createElement('td');
        idCell.textContent = account.id || '-';
        row.appendChild(idCell);
        
        // Логин/Телефон
        const loginCell = document.createElement('td');
        loginCell.textContent = account.login || account.phone || '-';
        row.appendChild(loginCell);
        
        // Лимиты запросов
        const limitsCell = document.createElement('td');
        limitsCell.textContent = account.request_limit || 'Не установлен';
        row.appendChild(limitsCell);
        
        // Использовано
        const usedCell = document.createElement('td');
        usedCell.textContent = account.requests_count || '0';
        row.appendChild(usedCell);
        
        // % использования
        const percentCell = document.createElement('td');
        if (account.request_limit) {
            const percent = Math.round((account.requests_count || 0) / account.request_limit * 100);
            percentCell.textContent = `${percent}%`;
            
            // Добавляем цветовое обозначение
            if (percent > 90) {
                percentCell.className = 'usage-critical';
            } else if (percent > 70) {
                percentCell.className = 'usage-warning';
            } else {
                percentCell.className = 'usage-normal';
            }
        } else {
            percentCell.textContent = '-';
        }
        row.appendChild(percentCell);
        
        // Последнее использование
        const lastUsedCell = document.createElement('td');
        if (account.last_used) {
            try {
                const lastUsed = new Date(account.last_used);
                // Проверяем валидность даты
                if (!isNaN(lastUsed.getTime())) {
                    lastUsedCell.textContent = lastUsed.toLocaleString();
                } else {
                    lastUsedCell.textContent = 'Никогда';
                }
            } catch (e) {
                console.error("Ошибка форматирования даты:", e, account.last_used);
                lastUsedCell.textContent = 'Никогда';
            }
        } else {
            lastUsedCell.textContent = 'Никогда';
        }
        row.appendChild(lastUsedCell);
        
        // Статус
        const statusCell = document.createElement('td');
        const statusSpan = document.createElement('span');
        statusSpan.className = 'account-status';
        
        if (account.active) {
            statusSpan.textContent = 'Активен';
            statusSpan.classList.add('status-active');
        } else {
            statusSpan.textContent = 'Неактивен';
            statusSpan.classList.add('status-inactive');
        }
        
        statusCell.appendChild(statusSpan);
        
        // Добавляем кнопку переключения статуса
        const toggleBtn = document.createElement('button');
        toggleBtn.className = 'toggle-status-btn';
        toggleBtn.innerHTML = account.active ? 
            '<i class="fas fa-toggle-on"></i>' : 
            '<i class="fas fa-toggle-off"></i>';
        
        toggleBtn.addEventListener('click', () => {
            toggleAccountStatus(account.id, platform, !account.active);
        });
        
        statusCell.appendChild(toggleBtn);
        row.appendChild(statusCell);
        
        tbody.appendChild(row);
    });
    
    table.appendChild(tbody);
    return table;
}

/**
 * Переключает статус аккаунта (активен/неактивен)
 * @param {String} accountId - ID аккаунта
 * @param {String} platform - платформа (vk/telegram)
 * @param {Boolean} newStatus - новый статус
 */
async function toggleAccountStatus(accountId, platform, newStatus) {
    console.log(`Изменение статуса аккаунта ${accountId} (${platform}) на ${newStatus ? 'активен' : 'неактивен'}`);
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        console.error('Админ-ключ не найден');
        return;
    }
    
    try {
        const response = await fetch('/api/admin/accounts/toggle_status', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-KEY': adminKey
            },
            body: JSON.stringify({
                account_id: accountId,
                platform: platform,
                active: newStatus
            })
        });
        
        if (!response.ok) {
            throw new Error(`Ошибка HTTP: ${response.status}`);
        }
        
        const result = await response.json();
        console.log('Результат изменения статуса:', result);
        
        // Перезагружаем статистику аккаунтов
        displayAccountsStats();
        
        showNotification(
            `Статус аккаунта ${accountId} изменен на ${newStatus ? 'активен' : 'неактивен'}`, 
            'success'
        );
        
    } catch (error) {
        console.error('Ошибка при изменении статуса аккаунта:', error);
        showNotification('Ошибка при изменении статуса: ' + error.message, 'error');
    }
}

// Вспомогательная функция для получения читаемого статуса
function getStatusLabel(status) {
    const statuses = {
        'active': 'Активен',
        'pending': 'Ожидает авторизации',
        'inactive': 'Неактивен',
        'error': 'Ошибка',
        'banned': 'Заблокирован',
        'rate_limited': 'Лимит запросов',
        'validation_required': 'Требуется валидация',
        'invalid': 'Недействителен'
    };
    
    return statuses[status] || status;
}

// Функция отображения аккаунтов (после обновления прокси)
async function displayAccounts() {
    console.log('Обновление данных об аккаунтах...');
    
    // Получаем админ-ключ
    const adminKey = getAdminKey();
    if (!adminKey) {
        console.error('Админ-ключ не найден');
        window.location.href = '/login';
        return;
    }
    
    try {
        // Обновляем список пользователей, так как аккаунты находятся внутри их структуры
        await displayUsers();
        showNotification('Данные успешно обновлены', 'success');
    } catch (error) {
        console.error('Ошибка при обновлении аккаунтов:', error);
        showNotification('Ошибка при обновлении данных', 'error');
    }
}

// Функция удаления аккаунта (обобщённая для Telegram и VK)
async function deleteAccount(platform, accountId, userId) {
    if (platform === 'telegram') {
        await deleteTelegramAccount(userId, accountId);
    } else if (platform === 'vk') {
        await deleteVkAccount(userId, accountId);
    }
}

/**
 * Сбрасывает статистику использования всех аккаунтов
 */
async function resetAccountsStats() {
    // Получаем админ-ключ
    const adminKey = getAdminKey();
    if (!adminKey) {
        showNotification('Ошибка: Админ-ключ не найден. Пожалуйста, войдите снова.', 'error');
        window.location.href = '/login';
        return;
    }
    
    try {
        // Показываем уведомление о начале процесса
        showNotification('Сброс статистики аккаунтов...', 'info');
        
        // Добавляем анимацию на кнопку сброса
        const resetBtn = document.querySelector('.reset-stats-btn');
        if (resetBtn) {
            const icon = resetBtn.querySelector('i');
            if (icon) {
                icon.className = 'fas fa-spinner fa-spin';
                resetBtn.disabled = true;
            }
        }
        
        const response = await fetch('/admin/accounts/reset-stats', {
            method: 'POST',
            headers: {
                'X-Admin-Key': adminKey,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({})
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error ${response.status}`);
        }
        
        const result = await response.json();
        
        // Восстанавливаем иконку кнопки
        if (resetBtn) {
            const icon = resetBtn.querySelector('i');
            if (icon) {
                icon.className = 'fas fa-undo';
                resetBtn.disabled = false;
            }
        }
        
        showNotification(
            `Статистика сброшена: ${result.reset_count} аккаунтов (${result.vk_updated} VK, ${result.tg_updated} Telegram)`, 
            'success'
        );
        
        // Обновляем статистику на странице после небольшой задержки
        setTimeout(async () => {
            await displayAccountsStats();
        }, 500);
    } catch (error) {
        console.error('Ошибка при сбросе статистики:', error);
        showNotification('Произошла ошибка при сбросе статистики аккаунтов: ' + error.message, 'error');
        
        // Восстанавливаем иконку кнопки в случае ошибки
        const resetBtn = document.querySelector('.reset-stats-btn');
        if (resetBtn) {
            const icon = resetBtn.querySelector('i');
            if (icon) {
                icon.className = 'fas fa-undo';
                resetBtn.disabled = false;
            }
        }
    }
} 