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
                        account.is_active
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
                        account.is_active
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

// Функция для создания элемента аккаунта
function createAccountItem(type, accountId, userId, displayName, status, isActive) {
    const accountItem = document.createElement('div');
    accountItem.className = 'account-item';
    accountItem.id = `account-${accountId}`;

    const accountInfo = document.createElement('div');
    accountInfo.className = 'account-info';
    
    const icon = document.createElement('i');
    icon.className = type === 'telegram' ? 'fab fa-telegram' : 'fab fa-vk';
    
    const name = document.createElement('span');
    name.textContent = displayName;
    
    const statusBadge = document.createElement('span');
    statusBadge.className = `account-status status-${status.toLowerCase()}`;
    statusBadge.textContent = getStatusText(status);
    
    accountInfo.appendChild(icon);
    accountInfo.appendChild(name);
    accountInfo.appendChild(statusBadge);
    
    const accountActions = document.createElement('div');
    accountActions.className = 'account-actions';
    
    // Кнопка проверки статуса
    const checkButton = document.createElement('button');
    checkButton.className = 'check-status-btn';
    checkButton.innerHTML = `<i class="fas fa-sync-alt"></i>`;
    checkButton.title = 'Проверить статус';
    checkButton.onclick = () => checkAccountStatus(type, accountId);
    
    // Кнопка переключения статуса активности
    const toggleButton = document.createElement('button');
    toggleButton.className = 'toggle-status-btn';
    toggleButton.innerHTML = isActive 
        ? `<i class="fas fa-toggle-on"></i>` 
        : `<i class="fas fa-toggle-off"></i>`;
    toggleButton.title = isActive ? 'Деактивировать' : 'Активировать';
    toggleButton.onclick = () => toggleAccountStatus(type, accountId, !isActive);
    
    // Кнопка удаления
    const deleteButton = document.createElement('button');
    deleteButton.className = 'delete-account-btn';
    deleteButton.innerHTML = `<i class="fas fa-trash-alt"></i>`;
    deleteButton.title = 'Удалить';
    deleteButton.onclick = () => deleteAccount(type, accountId, userId);
    
    accountActions.appendChild(checkButton);
    accountActions.appendChild(toggleButton);
    accountActions.appendChild(deleteButton);
    
    accountItem.appendChild(accountInfo);
    accountItem.appendChild(accountActions);
    
    return accountItem;
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

// Отображение уведомления
function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    
    let iconClass = 'fa-info-circle';
    if (type === 'success') iconClass = 'fa-check-circle';
    else if (type === 'error') iconClass = 'fa-exclamation-circle';
    else if (type === 'warning') iconClass = 'fa-exclamation-triangle';
    
    // Проверяем, содержит ли сообщение уже HTML-иконку
    const hasIcon = message.includes('<i class="fas');
    
    notification.innerHTML = `
        <div class="notification-content">
            ${hasIcon ? '' : `<i class="fas ${iconClass}"></i>`}
            <span>${message}</span>
        </div>
        <button class="close-notification">×</button>
    `;
    
    document.body.appendChild(notification);
    
    // Показываем уведомление с анимацией
    setTimeout(() => {
        notification.classList.add('show');
    }, 10);
    
    // Автоматически скрываем через 3 секунды
    const timeout = setTimeout(() => {
        closeNotification(notification);
    }, 3500);
    
    // Обработка закрытия уведомления
    const closeButton = notification.querySelector('.close-notification');
    closeButton.addEventListener('click', () => {
        clearTimeout(timeout);
        closeNotification(notification);
    });
}

// Закрытие уведомления
function closeNotification(notification) {
    notification.classList.remove('show');
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 300);
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
    const loadingIndicator = document.querySelector(`#account-${accountId} .loading-indicator`);
    if (loadingIndicator) loadingIndicator.style.display = 'inline-block';

    showNotification(`Проверка статуса аккаунта ${accountId}...`);

    // Получаем админ-ключ
    const adminKey = getAdminKey();
    if (!adminKey) {
        showNotification('Ошибка: Админ-ключ не найден. Пожалуйста, войдите снова.', 'error');
        window.location.href = '/login';
        if (loadingIndicator) loadingIndicator.style.display = 'none';
        return;
    }

    let endpoint = '';
    if (type === 'telegram') {
        endpoint = `/api/telegram/accounts/${accountId}/status`;
    } else if (type === 'vk') {
        endpoint = `/api/vk/accounts/${accountId}/status`;
    } else {
        showNotification('Неизвестный тип аккаунта', 'error');
        if (loadingIndicator) loadingIndicator.style.display = 'none';
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
            // Используем правильный селектор для элемента статуса
            const statusElement = document.querySelector(`#account-${accountId} .account-status`);
            
            // Проверяем, что элемент найден
            if (statusElement) {
                const newStatus = result.status;
                // Обновляем классы и текст элемента статуса
                statusElement.className = `account-status status-${newStatus.toLowerCase()}`;
                statusElement.textContent = getStatusText(newStatus);
                
                showNotification(`Статус аккаунта ${accountId} обновлен: ${getStatusText(newStatus)}`, 'success');

                // Обновляем данные пользователя, если необходимо
                // await displayUsers(); // Пока закомментируем, чтобы не было лишней перезагрузки
            } else {
                console.error(`Не найден элемент статуса для #account-${accountId} .account-status`);
                showNotification('Не удалось обновить интерфейс статуса', 'error');
            }
        } else {
            showNotification(`Ошибка проверки статуса (${response.status}): ${result.detail || 'Неизвестная ошибка'}`, 'error');
        }
    } catch (error) {
        console.error('Ошибка при проверке статуса:', error);
        showNotification('Ошибка сети при проверке статуса', 'error');
    } finally {
        if (loadingIndicator) loadingIndicator.style.display = 'none';
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
        usedCell.textContent = account.requests_made || '0';
        row.appendChild(usedCell);
        
        // % использования
        const percentCell = document.createElement('td');
        if (account.request_limit) {
            const percent = Math.round((account.requests_made || 0) / account.request_limit * 100);
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
            const lastUsed = new Date(account.last_used);
            lastUsedCell.textContent = lastUsed.toLocaleString();
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