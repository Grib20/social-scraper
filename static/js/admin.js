let currentUser;
let currentAccountId = null;
let currentPlatform = null;
let isSessionValid = false;

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
                'Authorization': `Bearer ${adminKey}`,
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
                'Authorization': `Bearer ${adminKey}`
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
            
            // Create a container for the buttons
            const telegramButtonContainer = document.createElement('div');
            telegramButtonContainer.className = 'account-header-actions'; // Add a class for styling

            const addTelegramButton = document.createElement('button');
            addTelegramButton.className = 'add-account-btn';
            addTelegramButton.innerHTML = `<i class="fas fa-plus"></i> Добавить`;
            addTelegramButton.onclick = () => showTelegramModal(user.id);
            telegramButtonContainer.appendChild(addTelegramButton); // Add button to container

            // === START NEW BUTTON ===
            const addSessionJsonButton = document.createElement('button');
            addSessionJsonButton.className = 'add-account-btn session-json-btn'; // Add specific class
            addSessionJsonButton.innerHTML = '<i class="fas fa-file-upload"></i> + Session/JSON'; // Text or Icon
            addSessionJsonButton.onclick = () => showSessionJsonModal(user.id);
            addSessionJsonButton.title = 'Добавить через .session и .json файлы'; // Tooltip
            telegramButtonContainer.appendChild(addSessionJsonButton); // Add button to container
            // === END NEW BUTTON ===

            telegramHeader.appendChild(telegramTitle);
            telegramHeader.appendChild(telegramButtonContainer); // Add the container to the header

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
function createAccountItem(platform, accountId, userId, displayName, status, isActive, proxy = null) {
    const accountIcon = platform === 'telegram' ? 'fab fa-telegram' : 'fab fa-vk';
    // Используем isActive для определения класса иконки статуса
    const statusClass = isActive ? 'active' : 'inactive'; 
    const statusText = getStatusText(status); // Получаем текст статуса (Активен, Ошибка и т.д.)
    const proxyText = proxy || 'Не установлен';
    
    // Форматирование даты последнего использования
    let lastUsedText = "Не использовался";
    let lastUsedClass = "inactive";
    
    // Создаем элемент аккаунта
    const accountItem = document.createElement('div');
    accountItem.className = 'account-item';
    accountItem.setAttribute('data-id', accountId);
    accountItem.setAttribute('data-platform', platform);
    
    // Сохраняем прокси в дата-атрибуте для использования в функции проверки
    if (proxy) {
        accountItem.setAttribute('data-proxy', proxy);
    }
    
    // Иконка и имя аккаунта
    const accountInfo = document.createElement('div');
    accountInfo.className = 'account-info';
    
    // Имя аккаунта с иконкой
    const accountName = document.createElement('div');
    accountName.className = 'account-name';
    
    // Иконка
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
    
    // Добавляем кнопки действий
    const accountActions = document.createElement('div');
    accountActions.className = 'account-actions';
    
    // Кнопка Включить/Выключить
    const toggleButton = document.createElement('button');
    toggleButton.className = `action-btn toggle-btn ${isActive ? 'active' : 'inactive'}`;
    toggleButton.innerHTML = isActive ? '<i class="fas fa-toggle-on"></i>' : '<i class="fas fa-toggle-off"></i>';
    toggleButton.title = isActive ? 'Выключить' : 'Включить';
    toggleButton.onclick = (event) => {
        event.stopPropagation(); // Предотвращаем всплытие события
        // Вызываем toggleAccountStatus БЕЗ третьего аргумента
        toggleAccountStatus(platform, accountId);
    };
    accountActions.appendChild(toggleButton);

    // Кнопка проверки статуса
    const checkButton = document.createElement('button');
    checkButton.className = 'action-btn check-btn';
    checkButton.innerHTML = '<i class="fas fa-sync-alt"></i>';
    checkButton.title = 'Проверить статус';
    checkButton.onclick = (event) => {
        event.stopPropagation(); // Предотвращаем всплытие события
        checkAccountStatus(platform, accountId);
    };
    accountActions.appendChild(checkButton);
    
    // Кнопка проверки прокси (если прокси установлен)
    if (proxy) {
        const checkProxyButton = document.createElement('button');
        checkProxyButton.className = 'action-btn check-proxy-btn';
        checkProxyButton.innerHTML = '<i class="fas fa-network-wired"></i>';
        checkProxyButton.title = 'Проверить прокси';
        checkProxyButton.onclick = (event) => {
            event.stopPropagation(); // Предотвращаем всплытие события
            checkProxyValidity(platform, accountId);
        };
        accountActions.appendChild(checkProxyButton);
    }
    
    // Добавляем кнопку "Запросить код" только для Telegram аккаунтов, которые не активны или в статусе ошибки/ожидания
    if (platform === 'telegram' && (status === 'pending' || status === 'pending_code' || status === 'error' || status === 'inactive')) {
        const requestCodeButton = document.createElement('button');
        requestCodeButton.className = 'action-btn request-code-btn';
        requestCodeButton.innerHTML = '<i class="fas fa-paper-plane"></i>';
        requestCodeButton.title = 'Запросить код авторизации';
        requestCodeButton.onclick = (event) => {
            event.stopPropagation();
            // Используем displayName, так как он содержит номер телефона для Telegram
            requestTelegramAuthCode(accountId, displayName); 
        };
        accountActions.appendChild(requestCodeButton);
    }

    // Кнопка редактирования
    const editButton = document.createElement('button');
    editButton.className = 'edit-account-btn';
    editButton.innerHTML = `<i class="fas fa-edit"></i>`;
    editButton.setAttribute('data-tooltip', 'Редактировать');
    if (platform === 'vk') {
        editButton.onclick = () => editVkAccount(userId, accountId);
    } else if (platform === 'telegram') {
        // Заменяем заглушку на вызов функции редактирования Telegram
        editButton.onclick = () => openEditTelegramModal(accountId);
    } else {
        // Можно оставить заглушку для других платформ, если они появятся
        editButton.onclick = () => alert(`Редактирование для ${platform} пока недоступно`);
    }
    
    // Кнопка удаления
    const deleteButton = document.createElement('button');
    deleteButton.className = 'delete-account-btn';
    deleteButton.innerHTML = `<i class="fas fa-trash-alt"></i>`;
    deleteButton.setAttribute('data-tooltip', 'Удалить');
    deleteButton.onclick = () => confirmDeleteAccount(platform, userId, accountId);
    
    accountActions.appendChild(editButton);
    accountActions.appendChild(deleteButton);
    
    accountItem.appendChild(accountActions);
    
    return accountItem;
}

// Функция для маскировки прокси (безопасность)
function maskProxy(proxy) {
    if (!proxy) return 'Не установлен';
    
    // Для прокси вида socks5://user:pass@host:port или http://user:pass@host:port
    const proxyParts = proxy.split('@');
    if (proxyParts.length === 2) {
        const authPart = proxyParts[0];
        const hostPart = proxyParts[1];
        
        // Маскируем пароль
        const authParts = authPart.split(':');
        if (authParts.length >= 3) {
            // Если есть протокол (socks5:// или http://)
            const protocol = authParts[0] + '://';
            const username = authParts[1];
            return `${protocol}${username}:******@${hostPart}`;
        } else if (authParts.length === 2) {
            // Если нет протокола
            const username = authParts[0];
            return `${username}:******@${hostPart}`;
        }
    }
    
    // Если нет аутентификации или прокси в другом формате
    return proxy;
}

// Функция для проверки валидности прокси
async function checkProxyValidity(platform, accountId) {
    try {
        const adminKey = getAdminKey();
        if (!adminKey) {
            showNotification('Админ-ключ не найден', 'error');
            window.location.href = '/login';
            return;
        }
        
        // Получаем данные аккаунта (особенно прокси)
        let proxyToCheck = null;
        
        // Находим элемент аккаунта
        const accountItem = document.querySelector(`.account-item[data-id="${accountId}"][data-platform="${platform}"]`);
        if (!accountItem) {
            showNotification('Аккаунт не найден на странице', 'error', 20000);
            return;
        }
        
        // Находим элемент с прокси внутри этого аккаунта
        const proxyValue = accountItem.querySelector('.proxy-value');
        if (!proxyValue || proxyValue.textContent === 'Не установлен') {
            showNotification('Прокси не установлен для этого аккаунта', 'warning', 20000);
            return;
        }
        
        // Получаем прокси из дата-атрибута
        proxyToCheck = accountItem.getAttribute('data-proxy');
        
        // Если прокси не найден в дата-атрибуте, запрашиваем с сервера
        if (!proxyToCheck) {
            // Показываем уведомление о запросе данных с сервера
            showNotification('Получение данных прокси с сервера...', 'info', 20000);
            
            // Запрашиваем данные о прокси для этого аккаунта
            const endpointUrl = `/api/${platform}/accounts/${accountId}/details`;
            const response = await fetch(endpointUrl, {
                headers: {
                    'Authorization': `Bearer ${adminKey}`
                }
            });
            
            if (response.ok) {
                const accountData = await response.json();
                proxyToCheck = accountData.proxy;
                
                if (!proxyToCheck) {
                    showNotification('Прокси не указан для этого аккаунта', 'warning', 20000);
                    return;
                }
            } else {
                showNotification('Не удалось получить данные аккаунта', 'error', 20000);
                return;
            }
        }
        
        // Теперь, когда у нас есть прокси, проверяем его
        showNotification('Проверка прокси...', 'info', 20000);
        
        const response = await fetch(`/api/v2/check-proxy`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${adminKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                proxy: proxyToCheck,
                platform: platform
            })
        });
        
        const result = await response.json();
        
        if (response.ok && result.valid) {
            showNotification(result.message || 'Прокси работает корректно', 'success', 20000);
        } else {
            showNotification(result.message || 'Проблема с прокси', 'error', 20000);
        }
    } catch (error) {
        console.error('Ошибка при проверке прокси:', error);
        showNotification('Ошибка при проверке прокси: ' + error.message, 'error', 20000);
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
    
    // Загружаем текущее значение прокси
    const adminKey = getAdminKey();
    if (adminKey) {
        fetch(`/api/${platform}/accounts/${accountId}/details`, {
            headers: {
                'Authorization': `Bearer ${adminKey}`
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.proxy) {
                document.getElementById('proxy_value').value = data.proxy;
            }
        })
        .catch(error => {
            console.error('Ошибка при загрузке данных прокси:', error);
        });
    }
    
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
        
        const adminKey = getAdminKey();
        if (!adminKey) {
            showNotification('Админ-ключ не найден', 'error');
            window.location.href = '/login';
            return;
        }
        
        const response = await fetch('/api/admin/update-proxy', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${adminKey}`
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
            showNotification(result.message || 'Прокси успешно обновлен', 'success');
            closeProxyModal();
            // Обновляем данные на странице
            displayUsers();
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
        
        const adminKey = getAdminKey();
        if (!adminKey) {
            showNotification('Админ-ключ не найден', 'error');
            window.location.href = '/login';
            return;
        }
        
        const response = await fetch('/api/admin/update-proxy', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${adminKey}`
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
            showNotification(result.message || 'Прокси успешно удален', 'success');
            closeProxyModal();
            // Обновляем данные на странице
            displayUsers();
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
    
    // Проверяем, что status не равен null или undefined
    if (!status) {
        return 'Неизвестно';
    }
    
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
function showNotification(message, type = 'info', duration = 10000) {
    console.log(`Уведомление (${type}): ${message}, Duration: ${duration}`);

    let notificationContainer = document.getElementById('notificationContainer');
    if (!notificationContainer) {
        notificationContainer = document.createElement('div');
        notificationContainer.id = 'notificationContainer';
        notificationContainer.style.position = 'fixed';
        notificationContainer.style.top = '20px';
        notificationContainer.style.right = '20px';
        notificationContainer.style.zIndex = '9999';
        document.body.appendChild(notificationContainer);
    }

    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.style.backgroundColor = type === 'error' ? '#ff4d4d' : 
                                      type === 'success' ? '#4CAF50' : 
                                      type === 'warning' ? '#ff9800' : '#2196F3';
    notification.style.color = '#fff';
    notification.style.padding = '15px 20px';
    notification.style.marginBottom = '10px';
    notification.style.borderRadius = '5px';
    notification.style.boxShadow = '0 4px 8px rgba(0,0,0,0.3)';
    notification.style.fontSize = '15px';
    notification.style.transition = 'opacity 0.5s ease, transform 0.5s ease'; // Обновили transition
    notification.style.opacity = '0'; // Начальная прозрачность для анимации
    notification.style.transform = 'translateX(100%)'; // Начальное положение для анимации
    notification.style.maxWidth = '350px';
    notification.style.wordWrap = 'break-word';
    notification.style.textAlign = 'left';
    notification.style.fontWeight = '500';
    notification.style.lineHeight = '1.4';
    notification.style.border = '1px solid rgba(255,255,255,0.2)';

    notification.innerHTML = message;

    let autoCloseTimeout = null;
    let fadeOutTimeout = null; // Отдельный таймер для анимации исчезновения

    // Функция плавного скрытия уведомления
    function fadeOutNotification() {
        clearTimeout(autoCloseTimeout); // Очищаем основной таймер
        clearTimeout(fadeOutTimeout);   // Очищаем таймер анимации, если он был
        
        notification.style.opacity = '0';
        notification.style.transform = 'translateX(100%)'; // Анимация ухода вправо
        
        // Удаляем элемент после завершения анимации
        fadeOutTimeout = setTimeout(() => {
            if (notification && notification.parentNode === notificationContainer) {
                notificationContainer.removeChild(notification);
            }
        }, 500); // Должно совпадать с временем transition
    }

    // При наведении курсора - отменяем таймер автозакрытия
    notification.onmouseover = function() {
        clearTimeout(autoCloseTimeout);
        autoCloseTimeout = null;
        // Можно добавить стили для hover, если нужно
        this.style.boxShadow = '0 8px 16px rgba(0,0,0,0.3)'; 
    };

    // При убирании курсора - запускаем таймер снова (с небольшой задержкой)
    notification.onmouseout = function() {
        this.style.boxShadow = '0 4px 8px rgba(0,0,0,0.3)';
        // Перезапускаем таймер только если он не был уже очищен кнопкой
        if (autoCloseTimeout !== -1) { // Используем -1 как флаг, что закрыто кнопкой
             autoCloseTimeout = setTimeout(fadeOutNotification, 500); // Небольшая задержка перед перезапуском
        }
    };

    // Кнопка закрытия
    const closeButton = document.createElement('span');
    closeButton.innerHTML = '&times;';
    closeButton.style.position = 'absolute';
    closeButton.style.top = '8px';
    closeButton.style.right = '10px';
    closeButton.style.cursor = 'pointer';
    closeButton.style.fontSize = '22px';
    closeButton.style.fontWeight = 'bold';
    closeButton.style.color = 'rgba(255,255,255,0.8)';
    closeButton.style.transition = 'color 0.2s';
    closeButton.onmouseover = function() { this.style.color = '#fff'; };
    closeButton.onmouseout = function() { this.style.color = 'rgba(255,255,255,0.8)'; };
    closeButton.onclick = function() {
        fadeOutNotification();
        autoCloseTimeout = -1; // Ставим флаг, что закрыто кнопкой
    };
    notification.appendChild(closeButton);

    notificationContainer.appendChild(notification);

    // Анимация появления (сдвиг слева и появление)
    // Небольшая задержка перед анимацией, чтобы браузер успел обработать начальные стили
    setTimeout(() => {
        notification.style.opacity = '1';
        notification.style.transform = 'translateX(0)';
    }, 50); 

    // Запускаем таймер автоматического закрытия
    autoCloseTimeout = setTimeout(fadeOutNotification, duration);
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
        resetTelegramModalToAddState();
    }
    
    // Сбрасываем состояние VK модального окна
    if (modalId === 'vkModal') {
        resetVkModal();
    }

    // Удаляем ID редактируемого аккаунта из формы Telegram, если он там был
    const telegramForm = document.getElementById('addTelegramForm');
    if (telegramForm && telegramForm.dataset && telegramForm.dataset.editingAccountId) {
        delete telegramForm.dataset.editingAccountId;
        console.log('Атрибут data-editing-account-id удален из формы Telegram.');
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
                'Authorization': `Bearer ${adminKey}`
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

// Обновленная функция: Открыть модальное окно для редактирования Telegram (с запросом данных)
async function openEditTelegramModal(accountId) {
    console.log(`Запрос данных для редактирования Telegram аккаунта: ${accountId}`);

    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    // --- ИЗМЕНЕНИЕ: Показываем глобальный лоадер или блокируем UI (пока пропустим для простоты) ---
    // Можно добавить: document.body.classList.add('loading-active');

    // Запрашиваем актуальные данные аккаунта с сервера
    try {
        const response = await fetch(`/api/telegram/accounts/${accountId}/details`, {
            headers: {
                'Authorization': `Bearer ${adminKey}`
            }
        });

        if (!response.ok) {
            // Пытаемся получить детали ошибки из JSON
            let errorDetail = `Ошибка HTTP: ${response.status}`;
            try {
                 const errorData = await response.json();
                 errorDetail = errorData.detail || errorDetail;
            } catch (jsonError) { /* игнорируем ошибку парсинга */ }
            throw new Error(errorDetail);
        }

        const accountData = await response.json();
        console.log("Получены данные аккаунта:", accountData);

        // --- ДАННЫЕ ПОЛУЧЕНЫ, ТЕПЕРЬ НАСТРАИВАЕМ МОДАЛЬНОЕ ОКНО ---

        // 1. Сбросить модальное окно в состояние "Добавления"
        // Передаем user_id, если он есть в ответе, иначе null
        resetTelegramModalToAddState(accountData.user_id || null);

        // 2. Получить ссылки на элементы формы ПОСЛЕ сброса
        const phoneInput = document.getElementById('phone');
        const apiIdInput = document.getElementById('api_id');
        const apiHashInput = document.getElementById('api_hash');
        const proxyInput = document.getElementById('proxy');
        const modalTitle = document.getElementById('telegramModalTitle');
        const form = document.getElementById('addTelegramForm');
        const submitButton = form ? form.querySelector('button[type="submit"]') : null;

        // Проверка наличия элементов формы
        if (!phoneInput || !apiIdInput || !apiHashInput || !proxyInput || !modalTitle || !form || !submitButton) {
           console.error("Не найдены все необходимые элементы формы в telegramModal для редактирования после сброса.");
           throw new Error("Ошибка интерфейса: структура модального окна нарушена.");
        }

        // 3. Предзаполнить поля и настроить режим редактирования
        phoneInput.value = accountData.phone || '';
        phoneInput.readOnly = true; // Запрещаем менять номер

        // Скрываем поля API ID и API Hash при редактировании
        apiIdInput.closest('.form-group').style.display = 'none';
        apiIdInput.value = accountData.api_id || ''; // Заполняем, если бэкенд отдает
        apiIdInput.readOnly = true;

        apiHashInput.closest('.form-group').style.display = 'none';
        apiHashInput.value = accountData.api_hash || ''; // Заполняем, если бэкенд отдает
        apiHashInput.readOnly = true;

        // Поле прокси
        proxyInput.value = accountData.proxy || '';
        proxyInput.readOnly = false; // Разрешаем менять прокси
        proxyInput.focus(); // Фокус на поле прокси

        // Скрываем ненужные блоки (авторизации)
        const authBlock = document.getElementById('telegramAuthBlock');
        if (authBlock) authBlock.style.display = 'none';
        const twoFABlock = document.getElementById('telegram2FABlock');
        if (twoFABlock) twoFABlock.style.display = 'none';

        // 4. Изменить заголовок и текст кнопки
        if (modalTitle) {
           modalTitle.innerHTML = '<i class="fab fa-telegram"></i> Редактировать аккаунт';
        }
        if (submitButton) {
           submitButton.innerHTML = '<i class="fas fa-save"></i> Сохранить изменения';
           submitButton.style.display = 'block';
           submitButton.disabled = false;
        }

        // 5. Добавить accountId в форму (как data-атрибут)
        form.dataset.editingAccountId = accountId;
        console.log(`Установлен data-editing-account-id: ${form.dataset.editingAccountId}`);

        // 6. Назначить обработчик для СОХРАНЕНИЯ изменений
        form.onsubmit = handleEditTelegramSubmit;
        console.log('Назначен обработчик handleEditTelegramSubmit');

        // --- ИЗМЕНЕНИЕ: Убираем глобальный лоадер ---
        // Можно добавить: document.body.classList.remove('loading-active');

        // 7. Показать настроенное модальное окно
        const modal = document.getElementById('telegramModal');
        if (modal) {
           modal.style.display = 'block';
        } else {
           console.error("Модальное окно telegramModal не найдено при попытке его показать.");
            throw new Error("Ошибка интерфейса: не найдено модальное окно.");
        }

    } catch (error) {
        console.error('Ошибка при подготовке к редактированию Telegram аккаунта:', error);
        showNotification(`Не удалось загрузить данные для редактирования: ${error.message}`, 'error');
        // --- ИЗМЕНЕНИЕ: Убираем глобальный лоадер в случае ошибки ---
        // Можно добавить: document.body.classList.remove('loading-active');
    }
}

// --- НОВАЯ ФУНКЦИЯ: Обработка сохранения изменений Telegram ---
async function handleEditTelegramSubmit(event) {
    event.preventDefault(); // Предотвращаем стандартную отправку
    console.log("Сработал обработчик handleEditTelegramSubmit");
    const form = event.target;

    // Проверяем, есть ли form и dataset
    if (!form || !form.dataset) {
         console.error("Ошибка: не найден элемент формы или его dataset при сохранении изменений.");
         showNotification("Ошибка интерфейса при сохранении.", "error");
         return;
    }
    const accountId = form.dataset.editingAccountId;
    const proxyInput = document.getElementById('proxy');
    // Получаем значение прокси, удаляем пробелы по краям, пустая строка или null становятся null
    const proxy = proxyInput ? (proxyInput.value.trim() || null) : null;

    if (!accountId) {
        showNotification('Ошибка: не удалось определить ID редактируемого аккаунта.', 'error');
        console.error("accountId не найден в form.dataset.editingAccountId при отправке");
        return;
    }

    console.log(`Сохранение изменений для Telegram аккаунта ${accountId}, новое прокси: ${proxy}`);

    const adminKey = getAdminKey();
    if (!adminKey) {
        // Перенаправляем на логин, если нет ключа
        window.location.href = '/login';
        return;
    }

    // Показываем индикатор загрузки на кнопке
    const submitButton = form.querySelector('button[type="submit"]');
    let originalButtonText = '<i class="fas fa-save"></i> Сохранить изменения';
    if (submitButton) {
        originalButtonText = submitButton.innerHTML;
        submitButton.disabled = true;
        submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Сохранение...';
    }

    try {
        // Отправляем PUT запрос на бэкенд
        const response = await fetch(`/api/telegram/accounts/${accountId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${adminKey}`
            },
            // Отправляем null, если прокси пустой, чтобы очистить его в БД
            body: JSON.stringify({ proxy: proxy })
        });

        // Восстанавливаем кнопку сразу после получения ответа (до обработки JSON)
        if (submitButton) {
            submitButton.disabled = false;
            submitButton.innerHTML = originalButtonText;
        }

        // Обработка ответа
        if (!response.ok) {
            // Пытаемся получить детали ошибки из JSON, если не получается - используем статус
            let errorDetail = `Ошибка HTTP: ${response.status}`;
            try {
                 const errorData = await response.json();
                 // Используем errorData.detail, если есть, иначе оставляем HTTP статус
                 errorDetail = errorData.detail || errorDetail;
            } catch (jsonError) {
                 // Если тело ответа не JSON или пустое
                 console.error("Не удалось разобрать JSON ошибки или тело пустое:", jsonError);
            }
            // Выбрасываем ошибку с полученным сообщением
            throw new Error(errorDetail);
        }

        // Успешный ответ (даже если тело пустое)
        let resultMessage = 'Изменения успешно сохранены.';
         try {
             const result = await response.json();
             resultMessage = result.message || resultMessage; // Используем сообщение из ответа, если оно есть
         } catch (jsonError) {
              console.log("Тело успешного ответа не является JSON или пустое.");
         }

        showNotification(resultMessage, 'success');
        closeModal('telegramModal'); // Закрываем окно после успеха (closeModal вызовет resetTelegramModalToAddState)
        displayUsers(); // Обновляем список пользователей, чтобы увидеть изменения

    } catch (error) {
        console.error('Ошибка при сохранении изменений Telegram аккаунта:', error);
        showNotification(`Ошибка сохранения: ${error.message}`, 'error');
        // Кнопка уже должна быть восстановлена выше, но на всякий случай
         if (submitButton && submitButton.disabled) {
             submitButton.disabled = false;
             submitButton.innerHTML = originalButtonText;
         }
    }
    // finally блок не нужен, так как очистка dataset и сброс обработчика теперь делаются в closeModal/resetTelegramModalToAddState
}

// Инициализация формы добавления VK-аккаунта
function initVkForm() {
    const addVkForm = document.getElementById('addVkForm');
    if (addVkForm) {
        addVkForm.addEventListener('submit', async (e) => {
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
                // Отправляем запрос на добавление аккаунта
                const response = await fetch('/api/vk/accounts', {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${adminKey}`,
                        'X-User-Id': formData.get('userId')
                    },
                    body: formData
                });
                
                const result = await response.json();
                
                // Восстанавливаем состояние кнопки
                submitButton.disabled = false;
                submitButton.innerHTML = originalButtonText;
                
                if (response.ok) {
                    // Если аккаунт успешно добавлен
                    closeModal('vkModal');
                    
                    // Обновляем список аккаунтов
                    await displayUsers();
                    
                    showNotification(result.message || 'Аккаунт VK успешно добавлен', 'success');
                } else {
                    // Если произошла ошибка
                    showNotification(result.detail || 'Ошибка при добавлении аккаунта VK', 'error');
                }
            } catch (error) {
                console.error('Ошибка при добавлении VK аккаунта:', error);
                showNotification(`Ошибка при добавлении аккаунта: ${error.message}`, 'error');
                
                // Восстанавливаем состояние кнопки
                submitButton.disabled = false;
                submitButton.innerHTML = originalButtonText;
            }
        });
    }
}

// Функция для сброса состояния VK модального окна
function resetVkModal() {
    // Сбрасываем форму
    const form = document.getElementById('addVkForm');
    if (form) {
        form.reset();
    }
}

// Инициализация обработчиков форм при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    console.log('Инициализация обработчиков форм...');
    
  
    // Инициализация формы добавления VK-аккаунта
    initVkForm();
    
    // Добавление глобальных стилей для тултипов
    const style = document.createElement('style');
    style.textContent = `
        /* Стили для тултипов */
        [data-tooltip] {
            position: relative;
            cursor: pointer;
        }
        
        [data-tooltip]::before {
            content: attr(data-tooltip);
            position: absolute;
            z-index: 1000;
            bottom: 125%;
            left: 50%;
            transform: translateX(-50%);
            padding: 5px 10px;
            white-space: nowrap;
            background-color: rgba(0, 0, 0, 0.8);
            color: white;
            border-radius: 4px;
            font-size: 12px;
            opacity: 0;
            visibility: hidden;
            transition: opacity 0.3s, visibility 0.3s;
        }
        
        [data-tooltip]:hover::before {
            opacity: 1;
            visibility: visible;
            transition-delay: 0.5s;
        }
        
        [data-tooltip]::after {
            content: '';
            position: absolute;
            z-index: 1000;
            bottom: 115%;
            left: 50%;
            transform: translateX(-50%);
            border: 5px solid transparent;
            border-top-color: rgba(0, 0, 0, 0.8);
            opacity: 0;
            visibility: hidden;
            transition: opacity 0.3s, visibility 0.3s;
        }
        
        [data-tooltip]:hover::after {
            opacity: 1;
            visibility: visible;
            transition-delay: 0.5s;
        }
    `;
    document.head.appendChild(style);
    
    // Добавление обработчиков для показа тултипов на сенсорных устройствах
    document.addEventListener('click', function(e) {
        const tooltip = e.target.closest('[data-tooltip]');
        if (tooltip) {
            e.preventDefault();
            e.stopPropagation();
            
            // Если тултип уже активен, не делаем ничего
            if (tooltip.classList.contains('tooltip-active')) {
                return;
            }
            
            // Убираем класс tooltip-active у всех элементов
            document.querySelectorAll('.tooltip-active').forEach(el => {
                el.classList.remove('tooltip-active');
            });
            
            // Добавляем класс tooltip-active
            tooltip.classList.add('tooltip-active');
            
            // Создаем и показываем тултип вручную для сенсорных устройств
            const tooltipText = tooltip.getAttribute('data-tooltip');
            let tooltipEl = document.getElementById('manual-tooltip');
            
            if (!tooltipEl) {
                tooltipEl = document.createElement('div');
                tooltipEl.id = 'manual-tooltip';
                tooltipEl.style.position = 'absolute';
                tooltipEl.style.backgroundColor = 'rgba(0, 0, 0, 0.8)';
                tooltipEl.style.color = 'white';
                tooltipEl.style.padding = '5px 10px';
                tooltipEl.style.borderRadius = '4px';
                tooltipEl.style.fontSize = '12px';
                tooltipEl.style.zIndex = '1001';
                document.body.appendChild(tooltipEl);
            }
            
            tooltipEl.textContent = tooltipText;
            
            // Позиционируем тултип
            const rect = tooltip.getBoundingClientRect();
            tooltipEl.style.left = rect.left + rect.width / 2 - tooltipEl.offsetWidth / 2 + 'px';
            tooltipEl.style.top = rect.top - tooltipEl.offsetHeight - 10 + 'px';
            tooltipEl.style.display = 'block';
            
            // Скрываем тултип через 5 секунд
            setTimeout(() => {
                tooltipEl.style.display = 'none';
                tooltip.classList.remove('tooltip-active');
            }, 5000);
        } else {
            // Если клик был вне тултипа, скрываем все тултипы
            document.querySelectorAll('.tooltip-active').forEach(el => {
                el.classList.remove('tooltip-active');
            });
            
            const manualTooltip = document.getElementById('manual-tooltip');
            if (manualTooltip) {
                manualTooltip.style.display = 'none';
            }
        }
    });
});

// Функция для редактирования VK аккаунта
function editVkAccount(userId, accountId) {
    // Получаем данные аккаунта
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }
    
    // Показываем модальное окно с загрузкой
    const modal = document.getElementById('vkModal');
    modal.style.display = 'block';
    
    const form = document.getElementById('addVkForm');
    if (form) {
        form.innerHTML = `
            <div class="loading">
                <i class="fas fa-spinner fa-spin"></i>
                <p>Загрузка данных аккаунта...</p>
            </div>
        `;
    }
    
    // Загружаем данные аккаунта
    fetch(`/api/vk/accounts/${accountId}/details`, {
        headers: {
            'Authorization': `Bearer ${adminKey}`,
            'X-User-Id': userId
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`Ошибка HTTP: ${response.status}`);
        }
        return response.json();
    })
    .then(accountData => {
        // Заполняем форму данными аккаунта
        form.innerHTML = `
            <input type="hidden" name="userId" value="${userId}">
            <input type="hidden" name="accountId" value="${accountId}">
            
            <div class="form-group">
                <label for="token"><i class="fas fa-key"></i> API токен:</label>
                <input type="text" id="token" name="token" placeholder="vk1.a.XcvBnMasd..." value="${accountData.token || ''}">
                <small>Оставьте пустым, если не хотите менять токен</small>
            </div>
            <div class="form-group">
                <label for="vk_proxy"><i class="fas fa-network-wired"></i> Прокси (опционально):</label>
                <input type="text" id="vk_proxy" name="proxy" placeholder="socks5://user:pass@host:port" value="${accountData.proxy || ''}">
                <small>Укажите прокси в формате socks5://user:pass@host:port или http://user:pass@host:port</small>
            </div>
            <div class="form-group">
                <label for="status"><i class="fas fa-toggle-on"></i> Статус:</label>
                <select id="status" name="status">
                    <option value="active" ${accountData.status === 'active' ? 'selected' : ''}>Активен</option>
                    <option value="inactive" ${accountData.status === 'inactive' ? 'selected' : ''}>Неактивен</option>
                </select>
            </div>
            <button type="submit"><i class="fas fa-save"></i> Сохранить</button>
        `;
        
        // Меняем обработчик отправки формы для обновления аккаунта
        form.onsubmit = function(e) {
            e.preventDefault();
            
            const formData = new FormData(form);
            
            // Показываем индикатор загрузки
            const submitButton = form.querySelector('button[type="submit"]');
            const originalButtonText = submitButton.innerHTML;
            submitButton.disabled = true;
            submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Сохранение...';
            
            // Создаем объект с данными для обновления
            const updateData = {};
            if (formData.get('token')) {
                updateData.token = formData.get('token');
            }
            if (formData.get('proxy')) {
                updateData.proxy = formData.get('proxy');
            }
            updateData.status = formData.get('status');
            
            // Отправляем запрос на обновление аккаунта
            fetch(`/api/vk/accounts/${accountId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${adminKey}`,
                    'X-User-Id': userId
                },
                body: JSON.stringify(updateData)
            })
            .then(response => {
                if (!response.ok) {
                    return response.json().then(error => {
                        throw new Error(error.detail || 'Ошибка при обновлении аккаунта');
                    });
                }
                return response.json();
            })
            .then(result => {
                // Закрываем модальное окно
                closeModal('vkModal');
                
                // Обновляем список аккаунтов
                displayUsers();
                
                // Показываем уведомление
                showNotification('Аккаунт VK успешно обновлен', 'success');
            })
            .catch(error => {
                console.error('Ошибка при обновлении аккаунта:', error);
                showNotification(`Ошибка при обновлении аккаунта: ${error.message}`, 'error');
                
                // Восстанавливаем состояние кнопки
                submitButton.disabled = false;
                submitButton.innerHTML = originalButtonText;
            });
        };
    })
    .catch(error => {
        console.error('Ошибка при загрузке данных аккаунта:', error);
        form.innerHTML = `
            <div class="error-message">
                <i class="fas fa-exclamation-triangle"></i>
                <p>Ошибка при загрузке данных аккаунта: ${error.message}</p>
            </div>
            <button type="button" onclick="closeModal('vkModal')">Закрыть</button>
        `;
    });
}

// Функция для подтверждения удаления аккаунта
function confirmDeleteAccount(platform, userId, accountId) {
    if (confirm(`Вы уверены, что хотите удалить этот аккаунт ${platform}?`)) {
        deleteAccount(platform, userId, accountId);
    }
}

// Функция для удаления аккаунта
function deleteAccount(platform, userId, accountId) {
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }
    
    // Отправляем запрос на удаление аккаунта
    fetch(`/api/${platform}/accounts/${accountId}`, {
        method: 'DELETE',
        headers: {
            'Authorization': `Bearer ${adminKey}`,
            'X-User-Id': userId
        }
    })
    .then(response => {
        if (!response.ok) {
            return response.json().then(error => {
                throw new Error(error.detail || `Ошибка при удалении аккаунта ${platform}`);
            });
        }
        return response.json();
    })
    .then(result => {
        // Обновляем список аккаунтов
        displayUsers();
        
        // Показываем уведомление
        showNotification(`Аккаунт ${platform} успешно удален`, 'success');
    })
    .catch(error => {
        console.error(`Ошибка при удалении аккаунта ${platform}:`, error);
        showNotification(`Ошибка при удалении аккаунта: ${error.message}`, 'error');
    });
}

// Функция для проверки статуса аккаунта
async function checkAccountStatus(platform, accountId) {
    try {
        const adminKey = getAdminKey();
        if (!adminKey) {
            showNotification('Админ-ключ не найден', 'error', 20000);
            window.location.href = '/login';
            return;
        }
        
        showNotification(`Проверка статуса аккаунта ${platform}...`, 'info', 20000);
        
        const response = await fetch(`/api/${platform}/accounts/${accountId}/status`, {
            headers: {
                'Authorization': `Bearer ${adminKey}`
            }
        });
        
        const result = await response.json();
        
        if (response.ok) {
            // Обновляем статус на странице
            const accountItem = document.querySelector(`.account-item[data-id="${accountId}"]`);
            if (accountItem) {
                const statusIndicator = accountItem.querySelector('.status-indicator');
                if (statusIndicator) {
                    statusIndicator.className = `status-indicator ${result.status === 'active' ? 'active' : 'inactive'}`;
                    statusIndicator.setAttribute('data-tooltip', `Статус: ${getStatusText(result.status)}`);
                }
                
                showNotification(`Статус аккаунта ${platform}: ${getStatusText(result.status)}`, 'success', 20000);
            } else {
                // Если элемент не найден (маловероятно, но возможно), все равно показываем уведомление
                showNotification(`Статус аккаунта ${platform} обновлен: ${getStatusText(result.status)}`, 'success', 20000);
            }
            
            // Убираем перезагрузку списка пользователей здесь, чтобы уведомление не пропадало
            // displayUsers(); 
        } else {
            showNotification(result.detail || `Ошибка при проверке статуса аккаунта ${platform}`, 'error', 20000);
        }
    } catch (error) {
        console.error(`Ошибка при проверке статуса аккаунта ${platform}:`, error);
        showNotification(`Ошибка при проверке статуса: ${error.message}`, 'error', 20000);
    }
}

// Функция для отображения статистики аккаунтов
async function displayAccountsStats() {
    console.log("Загрузка статистики аккаунтов...");
    const statsContainer = document.getElementById('accountsStatsContainer');

    if (!statsContainer) {
        console.error("Контейнер для статистики не найден");
        return;
    }

    statsContainer.innerHTML = `
        <div class="loading">
            <i class="fas fa-spinner fa-spin"></i>
            <p>Загрузка статистики...</p>
        </div>
    `;

    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    try {
        const response = await fetch('/api/admin/accounts/stats/detailed', {
            headers: {
                'Authorization': `Bearer ${adminKey}` // Используем Bearer токен
            }
        });

        if (!response.ok) {
             let errorDetail = `Ошибка HTTP: ${response.status}`;
             try {
                 const errorData = await response.json();
                 errorDetail = errorData.detail || errorDetail;
             } catch (e) {}
            throw new Error(errorDetail);
        }

        const stats = await response.json();
        console.log("Получены данные статистики:", stats);

        // ------ НАЧАЛО ИСПРАВЛЕНИЙ ------

        // Форматируем данные и отображаем статистику
        let html = `
            <div class="stats-summary">
                <div class="stats-card">
                    <div class="stats-icon"><i class="fab fa-telegram"></i></div>
                    <div class="stats-data">
                        <h4>Telegram</h4>
                        <div class="stats-details">
                            <div class="stats-item">
                                <span class="stats-label">Всего аккаунтов:</span>
                                <span class="stats-value">${stats.telegram?.stats_by_status?.total || 0}</span>
                            </div>
                            <div class="stats-item">
                                <span class="stats-label">Активные (в БД):</span>
                                <span class="stats-value">${stats.telegram?.stats_by_status?.active || 0}</span>
                            </div>
                            <div class="stats-item">
                                <span class="stats-label">Подключено (сейчас):</span>
                                <span class="stats-value">${Object.values(stats.telegram?.usage || {}).filter(acc => acc.connected).length}</span>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="stats-card">
                    <div class="stats-icon"><i class="fab fa-vk"></i></div>
                    <div class="stats-data">
                        <h4>VK</h4>
                        <div class="stats-details">
                            <div class="stats-item">
                                <span class="stats-label">Всего аккаунтов:</span>
                                <span class="stats-value">${stats.vk?.stats_by_status?.total || 0}</span>
                            </div>
                            <div class="stats-item">
                                <span class="stats-label">Активные (в БД):</span>
                                <span class="stats-value">${stats.vk?.stats_by_status?.active || 0}</span>
                            </div>
                            </div>
                    </div>
                </div>

                <div class="stats-card">
                    <div class="stats-icon"><i class="fas fa-users"></i></div>
                    <div class="stats-data">
                        <h4>Пользователи</h4>
                        <div class="stats-details">
                            <div class="stats-item">
                                <span class="stats-label">Всего пользователей:</span>
                                <span class="stats-value">${stats.users?.length || 0}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="stats-tables">
                <div class="stats-section">
                    <h3><i class="fab fa-telegram"></i> Статистика использования Telegram</h3>
                    <table class="stats-table">
                        <thead>
                            <tr>
                                <th>Пользователь</th>
                                <th>Телефон</th>
                                <th>Статус (БД)</th>
                                <th>Статус (Пул)</th>
                                <th>Запросы</th>
                                <th>Последнее исп.</th>
                            </tr>
                        </thead>
                        <tbody>
        `;

        // ИСПОЛЬЗУЕМ stats.telegram.usage для построения таблицы
        if (stats.telegram?.usage && Object.keys(stats.telegram.usage).length > 0) {
            // Сортируем аккаунты по ID или другому полю при необходимости
            const sortedTgAccounts = Object.values(stats.telegram.usage).sort((a, b) => (a.added_at || '').localeCompare(b.added_at || ''));

            for (const account of sortedTgAccounts) {
                const username = account.username || 'N/A';
                const accountName = account.phone || 'Неизвестный';
                const dbStatus = getStatusText(account.status); // Статус из БД
                const dbStatusClass = account.is_active ? 'active' : 'inactive';
                const poolStatus = `Подкл: ${account.connected ? 'Да' : 'Нет'} / Авториз: ${account.auth_status} / Дегр: ${account.degraded_mode ? 'Да' : 'Нет'}`;
                const requests = account.usage_count || 0;
                const lastUsed = account.last_used ? new Date(account.last_used).toLocaleString() : 'Не исп.';

                html += `
                    <tr>
                        <td>${username}</td>
                        <td>${accountName}</td>
                        <td><span class="status-badge ${dbStatusClass}">${dbStatus}</span></td>
                        <td>${poolStatus}</td>
                        <td>${requests}</td>
                        <td>${lastUsed}</td>
                    </tr>
                `;
            }
        } else {
            html += `<tr><td colspan="6" class="no-data">Нет данных об использовании Telegram</td></tr>`;
        }

        html += `
                    </tbody>
                </table>
            </div>

            <div class="stats-section">
                <h3><i class="fab fa-vk"></i> Статистика использования VK</h3>
                <table class="stats-table">
                    <thead>
                        <tr>
                            <th>Пользователь</th>
                            <th>Имя (VK)</th>
                            <th>Статус (БД)</th>
                            <th>Статус (Пул)</th>
                            <th>Запросы</th>
                            <th>Последнее исп.</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        // ИСПОЛЬЗУЕМ stats.vk.usage для построения таблицы
        if (stats.vk?.usage && Object.keys(stats.vk.usage).length > 0) {
            const sortedVkAccounts = Object.values(stats.vk.usage).sort((a, b) => (a.added_at || '').localeCompare(b.added_at || ''));

            for (const account of sortedVkAccounts) {
                const username = account.username || 'N/A';
                const accountName = account.user_name || `ID: ${account.id.substring(0, 8)}`;
                const dbStatus = getStatusText(account.status);
                const dbStatusClass = account.is_active ? 'active' : 'inactive';
                const poolStatus = `Дегр: ${account.degraded_mode ? 'Да' : 'Нет'}`;
                const requests = account.usage_count || 0;
                const lastUsed = account.last_used ? new Date(account.last_used).toLocaleString() : 'Не исп.';

                html += `
                    <tr>
                        <td>${username}</td>
                        <td>${accountName}</td>
                        <td><span class="status-badge ${dbStatusClass}">${dbStatus}</span></td>
                        <td>${poolStatus}</td>
                        <td>${requests}</td>
                        <td>${lastUsed}</td>
                    </tr>
                `;
            }
        } else {
            html += `<tr><td colspan="6" class="no-data">Нет данных об использовании VK</td></tr>`;
        }

        html += `
                    </tbody>
                </table>
            </div>

            <div class="stats-section">
                <h3><i class="fas fa-users"></i> Статистика по пользователям</h3>
                <table class="stats-table">
                    <thead>
                        <tr>
                            <th>Пользователь</th>
                            <th>Аккаунтов Telegram</th>
                            <th>Аккаунтов VK</th>
                            <th>Всего запросов (БД)</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        // Используем stats.users для этой таблицы
        if (stats.users && stats.users.length > 0) {
            for (const user of stats.users) {
                const username = user.username || 'N/A';
                const telegramCount = user.telegram_count || 0;
                const vkCount = user.vk_count || 0;
                const totalRequests = (user.telegram_requests || 0) + (user.vk_requests || 0);

                html += `
                    <tr>
                        <td>${username}</td>
                        <td>${telegramCount}</td>
                        <td>${vkCount}</td>
                        <td>${totalRequests}</td>
                    </tr>
                `;
            }
        } else {
            html += `<tr><td colspan="4" class="no-data">Нет данных</td></tr>`;
        }

        html += `
                    </tbody>
                </table>
            </div>

            <div class="stats-section">
                <h3><i class="fas fa-chart-area"></i> Статистика по статусам (из БД)</h3>
                <div class="stats-subsection">
                    <h4><i class="fab fa-telegram"></i> Telegram</h4>
                    <table class="stats-table">
                        <thead>
                            <tr>
                                <th>Статус</th>
                                <th>Количество</th>
                                <th>Среднее число запросов</th>
                                <th>Макс. запросов</th>
                                <th>Мин. запросов</th>
                            </tr>
                        </thead>
                        <tbody>
        `;

        // Используем stats.telegram.stats_by_status
        if (stats.telegram?.stats_by_status?.status_breakdown && stats.telegram.stats_by_status.status_breakdown.length > 0) {
            for (const status_stat of stats.telegram.stats_by_status.status_breakdown) {
                const statusText = getStatusText(status_stat.status);
                // Используем 'active'/'inactive' классы в зависимости от самого статуса, если возможно
                let statusClass = 'inactive';
                if (status_stat.status && status_stat.status.toLowerCase() === 'active') {
                    statusClass = 'active';
                } else if (status_stat.status && status_stat.status.toLowerCase().includes('error')) {
                     statusClass = 'error'; // Можно добавить стиль для ошибок
                }
                const count = status_stat.count || 0;
                const avgRequests = Math.round((status_stat.avg_requests || 0) * 100) / 100;
                const maxRequests = status_stat.max_requests || 0;
                const minRequests = status_stat.min_requests || 0;

                html += `
                    <tr>
                        <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                        <td>${count}</td>
                        <td>${avgRequests}</td>
                        <td>${maxRequests}</td>
                        <td>${minRequests}</td>
                    </tr>
                `;
            }
        } else {
            html += `<tr><td colspan="5" class="no-data">Нет данных</td></tr>`;
        }

        html += `
                    </tbody>
                </table>
            </div>

            <div class="stats-subsection">
                <h4><i class="fab fa-vk"></i> VK</h4>
                <table class="stats-table">
                    <thead>
                        <tr>
                            <th>Статус</th>
                            <th>Количество</th>
                            <th>Среднее число запросов</th>
                            <th>Макс. запросов</th>
                            <th>Мин. запросов</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        // Используем stats.vk.stats_by_status
        if (stats.vk?.stats_by_status?.status_breakdown && stats.vk.stats_by_status.status_breakdown.length > 0) {
            for (const status_stat of stats.vk.stats_by_status.status_breakdown) {
                const statusText = getStatusText(status_stat.status);
                let statusClass = 'inactive';
                 if (status_stat.status && status_stat.status.toLowerCase() === 'active') {
                     statusClass = 'active';
                 } else if (status_stat.status && status_stat.status.toLowerCase().includes('error')) {
                      statusClass = 'error';
                 }
                const count = status_stat.count || 0;
                const avgRequests = Math.round((status_stat.avg_requests || 0) * 100) / 100;
                const maxRequests = status_stat.max_requests || 0;
                const minRequests = status_stat.min_requests || 0;

                html += `
                    <tr>
                        <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                        <td>${count}</td>
                        <td>${avgRequests}</td>
                        <td>${maxRequests}</td>
                        <td>${minRequests}</td>
                    </tr>
                `;
            }
        } else {
            html += `<tr><td colspan="5" class="no-data">Нет данных</td></tr>`;
        }

        html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div> <!-- End of stats-tables -->
        `;

        // ------ КОНЕЦ ИСПРАВЛЕНИЙ ------

        statsContainer.innerHTML = html;

        // Добавляем время последнего обновления
        const updateTime = new Date().toLocaleString();
        const updateTimeElem = document.createElement('div');
        updateTimeElem.className = 'update-time';
        updateTimeElem.innerHTML = `<i class="fas fa-sync"></i> Последнее обновление: ${updateTime}`;
        statsContainer.appendChild(updateTimeElem);

    } catch (error) {
        console.error('Ошибка при загрузке статистики:', error);
        statsContainer.innerHTML = `
            <div class="error-message">
                <i class="fas fa-exclamation-triangle"></i>
                <p>Ошибка при загрузке статистики: ${error.message}</p>
                <button onclick="displayAccountsStats()" class="retry-button">
                    <i class="fas fa-sync"></i> Повторить
                </button>
            </div>
        `;
    }
}

// Функция для сброса статистики аккаунтов
async function resetAccountsStats() {
    if (!confirm('Вы уверены, что хотите сбросить статистику использования для всех аккаунтов?')) {
        return;
    }
    
    console.log("Сброс статистики аккаунтов...");
    
    const adminKey = getAdminKey();
    if (!adminKey) {
        showNotification('Админ-ключ не найден', 'error', 20000);
        window.location.href = '/login';
        return;
    }
    
    try {
        const response = await fetch('/api/admin/accounts/stats/reset', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${adminKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({})
        });
        
        if (!response.ok) {
            throw new Error(`Ошибка HTTP: ${response.status}`);
        }
        
        const result = await response.json();
        
        showNotification(result.message || 'Статистика аккаунтов успешно сброшена', 'success', 20000);
        
        // Перезагружаем статистику
        await displayAccountsStats();
    } catch (error) {
        console.error('Ошибка при сбросе статистики аккаунтов:', error);
        showNotification(`Ошибка при сбросе статистики: ${error.message}`, 'error', 20000);
    }
}

// Функция для запроса кода авторизации для существующего аккаунта
function requestTelegramAuthCode(accountId, phone) {
    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    showNotification('Запрос кода авторизации...', 'info');

    fetch(`/api/telegram/accounts/${accountId}/request-code`, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${adminKey}`
        }
    })
    .then(response => {
        // Восстанавливаем правильную проверку статуса HTTP
        if (!response.ok) {
            return response.json().then(error => {
                throw new Error(error.detail || `Ошибка HTTP: ${response.status}`);
            }).catch(() => {
                throw new Error(`Ошибка HTTP: ${response.status}`);
            });
        }
        return response.json();
    })
    .then(result => {
        // Изменяем проверку: считаем "pending_code" успехом для запроса кода
        if (result.status === 'pending_code') {
            showNotification(`Код отправлен на номер ${phone}. Введите его в появившемся окне.`, 'info');

            // Устанавливаем глобальный ID аккаунта
            currentTelegramAccountId = accountId;

            // Настраиваем и открываем telegramModal для ввода кода
            setupTelegramModalForCodeEntry(phone); // Вызываем новую вспомогательную функцию

        } else {
            throw new Error(result.detail || 'Не удалось запросить код');
        }
    })
    .catch(error => {
        console.error('Ошибка при запросе кода Telegram:', error);
        showNotification(`Ошибка запроса кода: ${error.message}`, 'error');
        currentTelegramAccountId = null; // Сбрасываем ID в случае ошибки
        // Здесь НЕ нужно вызывать resetTelegramModalToAddState, т.к. окно еще не открыто
    });
}

// НОВАЯ функция: Настройка telegramModal для ввода кода/2FA (для повторной авторизации)
function setupTelegramModalForCodeEntry(phone) {
    const modal = document.getElementById('telegramModal');
    // Если модальное окно не найдено, выходим
    if (!modal) {
        console.error("Элемент telegramModal не найден!");
        showNotification("Ошибка интерфейса: модальное окно не найдено.", "error");
        return;
    }

    const form = document.getElementById('addTelegramForm');
    const authBlock = document.getElementById('telegramAuthBlock');
    const twoFABlock = document.getElementById('telegram2FABlock');
    const phoneInput = document.getElementById('phone');
    const apiIdInput = document.getElementById('api_id');
    const apiHashInput = document.getElementById('api_hash');
    const proxyInput = document.getElementById('proxy');
    const userIdInput = form ? form.querySelector('input[name="userId"]') : null;
    const submitButton = form ? form.querySelector('button[type="submit"]') : null; // Кнопка "Добавить аккаунт"

    // Проверка наличия основных элементов внутри модального окна
    if (!form || !authBlock || !twoFABlock || !phoneInput) {
        console.error("Один из ключевых элементов внутри telegramModal не найден!", 
                        {form, authBlock, twoFABlock, phoneInput});
        showNotification("Ошибка интерфейса: структура модального окна нарушена.", "error");
        return;
    }

    // Сброс состояния блоков ввода кода/пароля
    authBlock.style.display = 'none';
    twoFABlock.style.display = 'none';
    const authCodeInput = document.getElementById('authCode');
    if (authCodeInput) authCodeInput.value = '';
    const twoFaPasswordInput = document.getElementById('two_fa_password');
    if (twoFaPasswordInput) twoFaPasswordInput.value = '';

    // Предзаполняем и блокируем поля
    if (userIdInput) userIdInput.value = ''; // userId не нужен для verify
    if (phoneInput) {
        phoneInput.value = phone;
        phoneInput.readOnly = true;
    }
    if (apiIdInput) {
        apiIdInput.value = '';
        apiIdInput.readOnly = true;
    }
    if (apiHashInput) {
        apiHashInput.value = '';
        apiHashInput.readOnly = true;
    }
     if (proxyInput) {
        proxyInput.value = '';
        proxyInput.readOnly = true;
    }

    // Показываем/скрываем нужные поля
    if (phoneInput) phoneInput.closest('.form-group').style.display = 'block';
    if (apiIdInput) apiIdInput.closest('.form-group').style.display = 'none';
    if (apiHashInput) apiHashInput.closest('.form-group').style.display = 'none';
    if (proxyInput) proxyInput.closest('.form-group').style.display = 'none';

    // Скрываем кнопку "Добавить аккаунт"
    if (submitButton) {
        submitButton.style.display = 'none';
    }

    // Показываем блок ввода кода
    authBlock.style.display = 'block';
    if (authCodeInput) authCodeInput.focus();

    // Назначаем обработчик для верификации кода
    if (form) {
         form.onsubmit = submitAuthCode;
    }

    // Устанавливаем заголовок ПЕРЕД показом окна
    const modalTitle = document.getElementById('telegramModalTitle');
    if (modalTitle) {
         modalTitle.textContent = 'Подтверждение входа Telegram';
    } else {
         console.error("Элемент telegramModalTitle не найден ПЕРЕД показом окна!");
    }

    // Показываем модальное окно
    modal.style.display = 'block';
}

// Вспомогательная функция для сброса telegramModal в состояние добавления
// --- Убедимся, что эта функция НЕ удалена --- 
function resetTelegramModalToAddState(userId = null) {
    // ... (код этой функции должен быть здесь)
    const modal = document.getElementById('telegramModal');
    const form = document.getElementById('addTelegramForm');
    const authBlock = document.getElementById('telegramAuthBlock');
    const twoFABlock = document.getElementById('telegram2FABlock');
    const phoneInput = document.getElementById('phone');
    const apiIdInput = document.getElementById('api_id');
    const apiHashInput = document.getElementById('api_hash');
    const proxyInput = document.getElementById('proxy');
    const userIdInput = form.querySelector('input[name="userId"]');
    const submitButton = form.querySelector('button[type="submit"]');

    // Сброс формы
    form.reset();

    // Скрытие блоков авторизации
    authBlock.style.display = 'none';
    twoFABlock.style.display = 'none';
    document.getElementById('authCode').value = '';
    document.getElementById('two_fa_password').value = '';

    // Сброс полей на редактируемые
    phoneInput.readOnly = false;
    apiIdInput.readOnly = false;
    apiHashInput.readOnly = false;
    proxyInput.readOnly = false;

    // Показ всех полей формы добавления
    phoneInput.closest('.form-group').style.display = 'block';
    apiIdInput.closest('.form-group').style.display = 'block';
    apiHashInput.closest('.form-group').style.display = 'block';
    proxyInput.closest('.form-group').style.display = 'block';

    // Показ кнопки submit
    if (submitButton) {
        submitButton.style.display = 'block'; // или 'inline-block'
        submitButton.disabled = false; // Убедимся, что кнопка активна
        submitButton.innerHTML = '<i class="fas fa-plus"></i> Добавить аккаунт'; // Возвращаем текст кнопки
    }

     // Установка User ID
    if (userIdInput && userId) {
        userIdInput.value = userId;
    }

    // Сброс глобальной переменной
    currentTelegramAccountId = null;

    // Сброс заголовка
     document.getElementById('telegramModalTitle').textContent = 'Добавить аккаунт Telegram';

    // Назначение обработчика для ДОБАВЛЕНИЯ аккаунта
    form.onsubmit = handleAddTelegramSubmit;
}

// Обработчик для формы ДОБАВЛЕНИЯ аккаунта
// --- Убедимся, что эта функция НЕ удалена --- 
function handleAddTelegramSubmit(event) {
    // ... (код этой функции должен быть здесь)
     event.preventDefault(); // Предотвращаем стандартную отправку формы

        // Получаем данные из формы
        const form = event.target; // Используем event.target
        const formData = new FormData(form);
        const userId = formData.get('userId');
        const phone = formData.get('phone');
        const apiId = formData.get('api_id');
        const apiHash = formData.get('api_hash');
        const proxy = formData.get('proxy') || null;

        if (!userId || !phone || !apiId || !apiHash) {
            showNotification('Все поля (кроме прокси) обязательны для заполнения.', 'warning');
            return;
        }

        const adminKey = getAdminKey();
        if (!adminKey) {
            window.location.href = '/login';
            return;
        }

        // Показываем индикатор загрузки
        const submitButton = form.querySelector('button[type="submit"]');
        const originalButtonText = submitButton.innerHTML;
        submitButton.disabled = true;
        submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Добавление...';

        // Отправляем данные на бэкенд
        fetch('/api/telegram/accounts', {
            method: 'POST',
            headers: {
                // НЕ указываем Content-Type, браузер сделает это сам для FormData
                'Authorization': `Bearer ${adminKey}`,
                'X-User-Id': userId // Заголовок с ID пользователя
            },
            body: formData // Отправляем объект FormData напрямую
        })
        .then(response => {
            // Восстанавливаем кнопку независимо от результата
            submitButton.disabled = false;
            submitButton.innerHTML = originalButtonText;
            // Проверяем статус перед чтением JSON
             if (!response.ok && response.status !== 409) { // 409 (Conflict) тоже может вернуть JSON с detail
                return response.json().then(error => {
                     throw new Error(error.detail || `Ошибка HTTP: ${response.status}`);
                }).catch(() => {
                     throw new Error(`Ошибка HTTP: ${response.status}`);
                });
             }
            return response.json().then(data => ({ status: response.status, body: data }));
        })
        .then(({ status, body }) => {
            // Условие для немедленно активного аккаунта: статус 200 или 201 и body.status 'active'
            if ((status === 200 || status === 201) && body.status === 'active') {
                 // Если статус 'active', значит аккаунт успешно добавлен (уже был и авторизован)
                 showNotification('Аккаунт Telegram успешно добавлен (уже авторизован).', 'success');
                 closeModal('telegramModal');
                 displayUsers(); // Обновляем список пользователей
            } else if (status === 200 && body.status === 'pending') {
                // Если статус 'pending', значит нужен код
                showNotification('Требуется код подтверждения. Введите его ниже.', 'info');
                currentTelegramAccountId = body.account_id; // Устанавливаем ID

                // Показываем блок для ввода кода, скрываем кнопку submit
                document.getElementById('telegramAuthBlock').style.display = 'block';
                document.getElementById('authCode').focus();
                if (submitButton) submitButton.style.display = 'none'; // Скрываем кнопку "Добавить"
                form.onsubmit = submitAuthCode; // Меняем обработчик на верификацию

            } else if (status === 201 && body.status === 'active') {
                 // Если статус 'active', значит аккаунт успешно добавлен (уже был и авторизован)
                 showNotification('Аккаунт Telegram успешно добавлен (уже авторизован).', 'success');
                 closeModal('telegramModal');
                 displayUsers(); // Обновляем список пользователей
            } else if (status >= 200 && status < 300 && body.status === 'pending_2fa') {
                // Если сразу требуется 2FA (новый аккаунт или существующий без сессии)
                showNotification('Аккаунт добавлен, но требуется пароль 2FA.', 'warning');
                 currentTelegramAccountId = body.account_id; // Устанавливаем ID

                 document.getElementById('telegram2FABlock').style.display = 'block';
                 document.getElementById('two_fa_password').focus();
                 if (submitButton) submitButton.style.display = 'none'; // Скрываем кнопку "Добавить"
                 form.onsubmit = submitAuthCode; // submitAuthCode обработает 2FA

            } else if (status === 409) {
                 // Обработка случая, когда аккаунт уже существует (Conflict)
                 showNotification(body.detail || 'Аккаунт с таким номером уже существует.', 'warning');
                 // Оставляем модальное окно открытым для исправления
                 form.onsubmit = handleAddTelegramSubmit; // Оставляем обработчик добавления
                 if (submitButton) submitButton.style.display = 'block'; // Показываем кнопку

            } else {
                 // Обрабатываем другие непредвиденные ошибки
                 throw new Error(body.detail || 'Неизвестная ошибка при добавлении аккаунта');
            }
        })
        .catch(error => {
            console.error('Ошибка при добавлении аккаунта Telegram:', error);
            showNotification(`Ошибка: ${error.message}`, 'error');
             // Восстанавливаем кнопку и обработчик
             if (submitButton) {
                 submitButton.disabled = false;
                 submitButton.innerHTML = originalButtonText;
                 submitButton.style.display = 'block';
             }
             document.getElementById('telegramAuthBlock').style.display = 'none';
             document.getElementById('telegram2FABlock').style.display = 'none';
             currentTelegramAccountId = null;
             form.onsubmit = handleAddTelegramSubmit;
        });
}

// Назначение ИСХОДНОГО обработчика для формы добавления
// --- Убедимся, что этот код НЕ удален --- 
const addTelegramFormElement = document.getElementById('addTelegramForm');
if (addTelegramFormElement) {
     addTelegramFormElement.onsubmit = handleAddTelegramSubmit;
}


// Функция открытия модального окна для ДОБАВЛЕНИЯ Telegram аккаунта
// --- Убедимся, что эта функция НЕ удалена и вызывает reset --- 
function openAddTelegramModal(userId) {
    resetTelegramModalToAddState(userId); // Используем функцию сброса
    document.getElementById('telegramModal').style.display = 'block';
    document.getElementById('phone').focus(); // Фокус на первое поле
}


// Функция для отправки кода авторизации (теперь для обоих случаев)
// --- Убедимся, что эта функция НЕ удалена --- 
function submitAuthCode(event) {
   console.log("submitAuthCode вызвана. Event:", event); // <-- ЛОГ 1: Вызвана ли функция?
   event.preventDefault();
    const form = event.target;
    const codeInput = document.getElementById('authCode');
    const code = codeInput.value;

    console.log(`submitAuthCode: Проверка ID аккаунта. currentTelegramAccountId = ${currentTelegramAccountId}`); // <-- ЛОГ 2: Значение ID
    if (!currentTelegramAccountId) {
        showNotification('Ошибка: Не найден ID аккаунта для верификации.', 'error');
        resetTelegramModalToAddState();
        return;
    }
     console.log(`submitAuthCode: Проверка кода. code = ${code}`); // <-- ЛОГ 3: Значение кода
     if (!code) {
        showNotification('Введите код авторизации.', 'warning');
        codeInput.focus();
        return;
    }

    const adminKey = getAdminKey();
    console.log(`submitAuthCode: Проверка ключа. adminKey = ${adminKey ? '***' : null}`); // <-- ЛОГ 4: Наличие ключа
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    const submitCodeButton = document.getElementById('submitAuthCodeBtn');
    let originalCodeButtonText = 'Подтвердить код'; // Default text
    if (submitCodeButton) {
        originalCodeButtonText = submitCodeButton.innerHTML;
        submitCodeButton.disabled = true;
        submitCodeButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Проверка...';
    }

    console.log(`submitAuthCode: Отправка fetch на /api/telegram/verify-code с account_id=${currentTelegramAccountId}`); // <-- ЛОГ 5: Перед fetch
    fetch('/api/telegram/verify-code', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${adminKey}`
        },
        body: JSON.stringify({
            account_id: currentTelegramAccountId,
            code: code
        })
    })
    .then(response => {
         if (submitCodeButton) {
            submitCodeButton.disabled = false;
            submitCodeButton.innerHTML = originalCodeButtonText;
         }
        if (response.status === 401) {
            return response.json().then(result => {
                if (result.status === 'pending_2fa') {
                    showNotification('Требуется пароль двухфакторной аутентификации.', 'warning');
                    document.getElementById('telegramAuthBlock').style.display = 'none';
                    document.getElementById('telegram2FABlock').style.display = 'block';
                    document.getElementById('two_fa_password').focus();
                } else {
                     throw new Error(result.detail || 'Ошибка аутентификации (401)');
                }
                return null; // Prevent further .then in case of 401
            });
        }
        if (!response.ok) {
            return response.json().then(error => {
                throw new Error(error.detail || `Ошибка при проверке кода (${response.status})`);
            });
        }
        return response.json();
    })
    .then(result => {
        if (result && result.status === 'active') {
            showNotification('Аккаунт Telegram успешно авторизован!', 'success');
            closeModal('telegramModal');
            displayUsers();
            currentTelegramAccountId = null;
        } else if (result) {
             throw new Error(result.detail || 'Неожиданный успешный ответ при проверке кода');
        }
    })
    .catch(error => {
        console.error('Ошибка при отправке кода авторизации:', error);
        showNotification(`Ошибка: ${error.message}`, 'error');
         if (submitCodeButton) {
            submitCodeButton.disabled = false;
            submitCodeButton.innerHTML = originalCodeButtonText;
         }
         // Возвращаем к вводу кода
         document.getElementById('telegramAuthBlock').style.display = 'block';
         document.getElementById('telegram2FABlock').style.display = 'none';
         if(codeInput) codeInput.focus();
    });
}

// Обработчик для кнопки подтверждения 2FA пароля
// --- Убедимся, что эта функция и ее назначение НЕ удалены --- 
const submit2FAButton = document.getElementById('submit2FA');
if (submit2FAButton) {
    submit2FAButton.addEventListener('click', submit2FAPassword);
}

function submit2FAPassword() {
    // ... (код этой функции должен быть здесь)
    const passwordInput = document.getElementById('two_fa_password');
    const password = passwordInput.value;
    const code = document.getElementById('authCode').value;

    if (!currentTelegramAccountId) {
        showNotification('Ошибка: Не найден ID аккаунта для верификации 2FA.', 'error');
        resetTelegramModalToAddState();
        return;
    }
    if (!password) {
        showNotification('Введите пароль 2FA.', 'warning');
        passwordInput.focus();
        return;
    }
     if (!code) {
        showNotification('Ошибка: Код авторизации отсутствует для 2FA.', 'error');
         document.getElementById('telegramAuthBlock').style.display = 'block';
         document.getElementById('telegram2FABlock').style.display = 'none';
         document.getElementById('authCode').focus();
         return;
    }

    const adminKey = getAdminKey();
    if (!adminKey) {
        window.location.href = '/login';
        return;
    }

    const submitButton = document.getElementById('submit2FA');
    const originalButtonText = submitButton.innerHTML;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Проверка...';

    fetch('/api/telegram/verify-code', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${adminKey}`
        },
        body: JSON.stringify({
            account_id: currentTelegramAccountId,
            code: code,
            password: password
        })
    })
    .then(response => {
        submitButton.disabled = false;
        submitButton.innerHTML = originalButtonText;
        if (!response.ok) {
            return response.json().then(error => {
                throw new Error(error.detail || `Ошибка при проверке пароля 2FA (${response.status})`);
            });
        }
        return response.json();
    })
    .then(result => {
        if (result.status === 'active') {
            showNotification('Аккаунт Telegram успешно авторизован!', 'success');
            closeModal('telegramModal');
            displayUsers();
            currentTelegramAccountId = null;
        } else {
             throw new Error(result.detail || 'Не удалось авторизоваться с паролем 2FA (неожиданный ответ)');
        }
    })
    .catch(error => {
        console.error('Ошибка при отправке пароля 2FA:', error);
        showNotification(`Ошибка 2FA: ${error.message}`, 'error');
         passwordInput.focus();
         passwordInput.select();
    });
}

async function deleteUser(userId) { 
    // userId здесь будет содержать api_key пользователя, который передается из displayUsers
    if (!userId) {
        console.error("Попытка удалить пользователя без ID (API ключа).");
        showNotification('Ошибка: Не удалось определить ID пользователя для удаления.', 'error');
        return;
    }

    // Используем userId (api_key) в сообщении
    if (!confirm(`Вы уверены, что хотите удалить пользователя с API ключом: ${userId}? \nВНИМАНИЕ: Все связанные с ним аккаунты VK и Telegram также будут удалены!`)) {
        return; // Пользователь отменил удаление
    }

    console.log(`Попытка удаления пользователя с API ключом: ${userId}`);

    const adminKey = getAdminKey();
    if (!adminKey) {
        showNotification('Админ-ключ не найден. Авторизуйтесь снова.', 'error');
        window.location.href = '/login';
        return;
    }

    try {
        // Отправляем DELETE запрос на эндпоинт, который у вас есть
        // Передаем userId (api_key) в URL
        const response = await fetch(`/admin/users/${userId}`, { 
            method: 'DELETE',
            headers: {
                // Используем 'Authorization': 'Bearer ...' если ваш эндпоинт его ожидает,
                // или 'X-Admin-Key', если он ожидает его (судя по коду эндпоинта, он проверяет оба)
                'Authorization': `Bearer ${adminKey}` 
                // 'X-Admin-Key': adminKey // Если используете этот заголовок
            }
        });

        if (!response.ok) {
            let errorDetail = `Ошибка HTTP: ${response.status}`;
            try {
                const errorData = await response.json();
                errorDetail = errorData.detail || errorDetail;
            } catch (jsonError) { /* игнорируем */ }
            throw new Error(errorDetail);
        }

        let resultMessage = 'Пользователь успешно удален.';
        try {
            const result = await response.json();
            resultMessage = result.message || resultMessage;
        } catch (jsonError) { /* игнорируем пустое тело ответа */ }

        showNotification(resultMessage, 'success');
        displayUsers(); // Обновляем список пользователей

    } catch (error) {
        console.error('Ошибка при удалении пользователя:', error);
        showNotification(`Ошибка удаления: ${error.message}`, 'error');
    }
}

// ... existing code ...

// --- НОВАЯ ФУНКЦИЯ: Переключение статуса аккаунта --- 
async function toggleAccountStatus(platform, accountId) {
    console.log(`Переключение статуса для ${platform}:${accountId}...`);

    const adminKey = getAdminKey();
    if (!adminKey) {
        showNotification('Админ-ключ не найден. Авторизуйтесь снова.', 'error');
        window.location.href = '/login';
        return;
    }

    // Находим кнопку
    const accountItem = document.querySelector(`.account-item[data-id="${accountId}"][data-platform="${platform}"]`);
    const toggleButton = accountItem ? accountItem.querySelector('.toggle-btn') : null;
    
    if (!toggleButton) {
        console.error(`Кнопка для ${platform}:${accountId} не найдена.`);
        showNotification('Ошибка: кнопка переключения не найдена', 'error');
        return;
    }

    // Определяем ТЕКУЩЕЕ состояние по классу кнопки
    const currentIsActive = toggleButton.classList.contains('active');
    // Вычисляем НОВОЕ желаемое состояние
    const newActiveState = !currentIsActive;
    console.log(`   Текущее состояние: ${currentIsActive}, Желаемое новое: ${newActiveState}`);

    let originalButtonHtml = toggleButton.innerHTML;
    toggleButton.disabled = true;
    toggleButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';

    try {
        const response = await fetch('/api/admin/accounts/toggle_status', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${adminKey}`
            },
            body: JSON.stringify({
                platform: platform,
                account_id: accountId,
                active: newActiveState // Отправляем новое состояние на бэкенд
            })
        });

        // Восстанавливаем кнопку сразу после ответа, но перед проверкой ok
        toggleButton.disabled = false;

        if (!response.ok) {
            let errorDetail = `Ошибка HTTP: ${response.status}`;
            try {
                const errorData = await response.json();
                errorDetail = errorData.detail || errorDetail;
            } catch (e) { /* ignore json parsing error */ }
            throw new Error(errorDetail);
        }

        const result = await response.json();
        showNotification(result.message || 'Статус аккаунта успешно изменен', 'success');

        // Обновляем кнопку на основе newActiveState
        toggleButton.innerHTML = newActiveState ? '<i class="fas fa-toggle-on"></i>' : '<i class="fas fa-toggle-off"></i>';
        toggleButton.title = newActiveState ? 'Выключить' : 'Включить';
        // Обновляем классы
        toggleButton.classList.remove(currentIsActive ? 'active' : 'inactive');
        toggleButton.classList.add(newActiveState ? 'active' : 'inactive');

        // Обновляем UI для статус индикатора (рядом с именем)
        if (accountItem) {
            const statusIndicator = accountItem.querySelector('.status-indicator');
            if (statusIndicator) {
                statusIndicator.classList.remove(currentIsActive ? 'active' : 'inactive');
                statusIndicator.classList.add(newActiveState ? 'active' : 'inactive');
            }
        }

    } catch (error) {
        console.error('Ошибка при переключении статуса аккаунта:', error);
        showNotification(`Ошибка переключения статуса: ${error.message}`, 'error');
        // Восстанавливаем кнопку в исходное состояние (currentIsActive) В СЛУЧАЕ ОШИБКИ
        toggleButton.innerHTML = originalButtonHtml; // Используем сохраненный HTML
        // Классы не менялись, так что восстанавливать их не нужно
    } 
}

// ... existing code ...

// === Новые функции для добавления через Session+JSON ===

// Показывает модальное окно для загрузки Session+JSON
function showSessionJsonModal(userId) {
    currentUser = userId; // Сохраняем ID текущего пользователя
    
    const modal = document.getElementById('sessionJsonModal');
    const form = document.getElementById('addSessionJsonForm');
    const userIdInput = document.getElementById('sessionJsonUserId');

    if (modal && form && userIdInput) {
        form.reset(); // Сбрасываем форму
        userIdInput.value = userId; // Устанавливаем ID пользователя
        modal.style.display = 'block'; // Показываем окно
    } else {
        console.error("Не удалось найти элементы модального окна Session+JSON");
        showNotification("Ошибка: Не удалось открыть окно добавления аккаунта.", "error");
    }
}

// Обрабатывает отправку формы Session+JSON
async function handleAddSessionJsonSubmit(event) {
    console.log('--- handleAddSessionJsonSubmit CALLED ---'); // Moved to the very top
    event.preventDefault(); 
    
    // Find the form the button belongs to
    const button = event.currentTarget; // The button that was clicked
    const form = button.closest('form'); // Find the closest parent form
    if (!form) {
        console.error("Could not find parent form for the clicked button!");
        showNotification("Критическая ошибка: не найдена форма для кнопки.", "error");
        return; // Stop execution if form is not found
    }
    
    const formData = new FormData(form);
    const adminKey = getAdminKey();
    // Получаем api_key пользователя из скрытого поля
    const userApiKey = formData.get('userId'); 

    if (!adminKey) {
        showNotification("Ошибка: Админ-ключ не найден.", "error");
        return;
    }
    if (!userApiKey) {
         showNotification("Ошибка: Не удалось определить пользователя.", "error");
         return;
    }

    const sessionFileInput = document.getElementById('session_file_input');
    const jsonFileInput = document.getElementById('json_file_input');

    if (!sessionFileInput.files.length || !jsonFileInput.files.length) {
        showNotification("Пожалуйста, выберите оба файла (.session и .json)", "warning");
        return;
    }

    // Добавляем файлы в FormData (уже должны быть там из new FormData(form))
    // formData.append('session_file', sessionFileInput.files[0]);
    // formData.append('json_file', jsonFileInput.files[0]);

    // Добавляем прокси (Form параметр, тоже должен быть в formData)
    // const proxyValue = document.getElementById('session_json_proxy').value;
    // if (proxyValue) {
    //     formData.append('proxy', proxyValue);
    // }

    console.log('--- handleAddSessionJsonSubmit ---');
    console.log('Admin Key:', adminKey ? 'Present' : 'MISSING!');
    console.log('User API Key:', userApiKey ? userApiKey : 'MISSING!');
    console.log('Session File Selected:', sessionFileInput.files.length > 0);
    console.log('JSON File Selected:', jsonFileInput.files.length > 0);
    // Выведем все данные формы для отладки
    console.log('FormData Entries:');
    for (let [key, value] of formData.entries()) { 
        console.log(key, value); 
    }
    console.log('----------------------------------');

    // const submitButton = form.querySelector('button[type="submit"]'); // Old selector based on type
    const submitButton = document.getElementById('addSessionJsonSubmitButton'); // Find button by ID
    if (!submitButton) {
        console.error("Could not find submit button with ID addSessionJsonSubmitButton!");
        // We might not need to show a user notification here if the form couldn't be found earlier
        return; 
    }

    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Добавление...';

    try {
        const response = await fetch('/api/v1/telegram/accounts/upload_session_json', {
            method: 'POST',
            headers: {
                // 'Content-Type': 'multipart/form-data', // Browser sets this with boundary
                'X-Admin-Key': adminKey, // Keep admin key if needed elsewhere
                'api-key': userApiKey // Correct header for user API key
            },
            body: formData
        });

        const result = await response.json();

        if (response.ok) {
            showNotification(result.message || 'Аккаунт успешно добавлен!', 'success');
            closeModal('sessionJsonModal');
            displayUsers(); // Обновляем список пользователей
        } else {
            showNotification(result.detail || 'Ошибка при добавлении аккаунта.', 'error');
        }
    } catch (error) {
        console.error('Ошибка при добавлении аккаунта (Session+JSON):', error);
        showNotification('Произошла сетевая ошибка при добавлении аккаунта.', 'error');
    } finally {
        submitButton.disabled = false;
        submitButton.innerHTML = '<i class="fas fa-plus-circle"></i> Добавить аккаунт';
    }
}

// === Конец новых функций ===

// --- START MISSING FUNCTION ---
async function handleAddVkSubmit(event) {
    event.preventDefault();
    const form = event.target;
    const formData = new FormData(form);
    const adminKey = getAdminKey();
    const userId = formData.get('userId'); // Assuming hidden field exists
    const token = formData.get('token');
    const proxy = formData.get('vk_proxy'); // Check name in HTML

    if (!adminKey || !userId) {
        showNotification("Ошибка: Не удалось определить ключ или пользователя.", "error");
        return;
    }
    if (!token) {
         showNotification("Ошибка: Токен VK обязателен.", "error");
         return;
    }

    const submitButton = form.querySelector('button[type="submit"]');
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Добавление...';

    try {
        // Determine the correct endpoint - Assuming an admin endpoint exists like for Telegram
        // Might need adjustment based on actual backend routes
        const response = await fetch(`/admin/users/${userId}/vk`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Admin-Key': adminKey
            },
            body: JSON.stringify({
                token: token,
                proxy: proxy || null // Send null if empty
            })
        });

        const result = await response.json();

        if (response.ok) {
            showNotification(result.message || 'VK аккаунт успешно добавлен!', 'success');
            closeModal('vkModal');
            displayUsers(); // Refresh user list
        } else {
            showNotification(result.detail || 'Ошибка при добавлении VK аккаунта.', 'error');
        }
    } catch (error) {
        console.error('Ошибка при добавлении VK аккаунта:', error);
        showNotification('Произошла сетевая ошибка при добавлении VK аккаунта.', 'error');
    } finally {
        submitButton.disabled = false;
        submitButton.innerHTML = '<i class="fas fa-plus-circle"></i> Добавить';
    }
}
// --- END MISSING FUNCTION ---

// Обработчик события загрузки DOM
document.addEventListener('DOMContentLoaded', () => {
    // Получаем админ-ключ из localStorage при загрузке
    const savedAdminKey = localStorage.getItem('adminKey');
    if (savedAdminKey) {
        // Если ключ есть, можно сразу попробовать отобразить пользователей
        // или выполнить другие действия, требующие ключа
        // displayUsers(); 
    } else {
        // Если ключа нет, возможно, перенаправить на логин или скрыть админ-элементы
        // window.location.href = '/login';
    }

    // Инициализация отображения пользователей
    displayUsers();

    // Обработчик для формы добавления пользователя
    const addUserForm = document.getElementById('addUserForm');
    if (addUserForm) {
        addUserForm.addEventListener('submit', registerUser);
    }

    // Обработчик для формы добавления Telegram аккаунта (старый способ)
    const addTelegramForm = document.getElementById('addTelegramForm');
    if (addTelegramForm) {
        addTelegramForm.addEventListener('submit', handleAddTelegramSubmit);
    }

    // Обработчик для формы добавления VK аккаунта
    const addVkForm = document.getElementById('addVkForm');
    if (addVkForm) {
        addVkForm.addEventListener('submit', handleAddVkSubmit); 
    }
    
    // Обработчик для формы подтверждения кода Telegram
    const authForm = document.getElementById('telegramAuthForm');
    if (authForm) {
        authForm.addEventListener('submit', submitAuthCode);
    }

    // Обработчик для кнопки подтверждения 2FA
    const submit2FAButton = document.getElementById('submit2FA');
    if (submit2FAButton) {
        submit2FAButton.addEventListener('click', submit2FAPassword);
    }

    // Обработчик для формы изменения прокси
    const changeProxyForm = document.getElementById('changeProxyForm');
    if (changeProxyForm) {
        changeProxyForm.addEventListener('submit', updateProxy);
    }

    // УДАЛЯЕМ старый обработчик для формы submit
    // const sessionJsonForm = document.getElementById('addSessionJsonForm');
    // if (sessionJsonForm) {
    //     sessionJsonForm.addEventListener('submit', handleAddSessionJsonSubmit);
    // } else {
    //      console.error("Форма addSessionJsonForm не найдена!");
    // }
    
    // НАЗНАЧАЕМ новый обработчик на КЛИК кнопки
    const addSessionJsonButton = document.getElementById('addSessionJsonSubmitButton');
    if (addSessionJsonButton) {
        addSessionJsonButton.addEventListener('click', handleAddSessionJsonSubmit);
    } else {
        console.error("Кнопка addSessionJsonSubmitButton не найдена!");
    }
    
    // Инициализация вкладок
    const activeTab = localStorage.getItem('activeTab') || 'users';
    switchTab(activeTab); // Активируем сохраненную вкладку
});



// ... остальной код admin.js ...