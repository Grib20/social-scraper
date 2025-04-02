let currentUser;

// Переменные для хранения информации об аккаунте Telegram в процессе авторизации
let currentTelegramAccountId = null;

// Функция для сохранения админ-ключа
function saveAdminKey(adminKey) {
    // Сохраняем в localStorage
    localStorage.setItem('adminKey', adminKey);
    
    // Устанавливаем cookie на всякий случай
    document.cookie = `admin_key=${adminKey}; path=/; expires=Fri, 31 Dec 9999 23:59:59 GMT`;
    
    console.log('AdminKey сохранен:', adminKey);
}

// Функция для получения админ-ключа
function getAdminKey() {
    // Сначала пробуем получить из localStorage
    let adminKey = localStorage.getItem('adminKey');
    
    // Если нет в localStorage, пробуем получить из cookie
    if (!adminKey) {
        const cookieValue = document.cookie
            .split('; ')
            .find(row => row.startsWith('admin_key='));
        if (cookieValue) {
            adminKey = cookieValue.split('=')[1];
            // Если нашли в cookie, сохраняем в localStorage
            if (adminKey) {
                localStorage.setItem('adminKey', adminKey);
            }
        }
    }
    
    return adminKey;
}

// Проверка авторизации при загрузке страницы
document.addEventListener('DOMContentLoaded', async () => {
    console.log('Страница загружена');
    
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

    try {
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
        await displayUsers();
        
        // Подключаем обработчик для формы добавления пользователя
        document.getElementById('addUserForm').addEventListener('submit', registerUser);
    } catch (error) {
        console.error('Ошибка проверки авторизации:', error);
        localStorage.removeItem('adminKey');
        window.location.href = '/login';
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
        
        if (response.ok) {
            const users = await response.json();
            if (users.length === 0) {
                usersContainer.innerHTML = '<p>Нет пользователей</p>';
                return;
            }
            
            let html = '';
            
            users.forEach(user => {
                console.log('User data:', user); // Логирование данных для отладки
                
                const telegramAccountsHtml = user.telegram_accounts ? user.telegram_accounts.map(account => `
                    <div class="account-item">
                        <div class="account-info">
                            <i class="fab fa-telegram"></i>
                            <span>${account.phone || 'Неизвестный'}</span>
                            <span class="account-status ${account.status === 'active' ? 'status-active' : 'status-pending'}">
                                ${account.status === 'active' ? 'Активен' : 'Ожидает авторизации'}
                            </span>
                        </div>
                        <button onclick="deleteTelegramAccount('${user.id}', '${account.phone}')" class="delete-account-btn">
                            <i class="fas fa-trash-alt"></i>
                        </button>
                    </div>
                `).join('') : '';
                
                const vkAccountsHtml = user.vk_accounts ? user.vk_accounts.map(account => `
                    <div class="account-item">
                        <div class="account-info">
                            <i class="fab fa-vk"></i>
                            <span>Токен: ${maskToken(account.token || '')}</span>
                            <span class="account-status ${account.status === 'active' ? 'status-active' : 'status-pending'}">
                                ${account.status === 'active' ? 'Активен' : 'Ожидает авторизации'}
                            </span>
                        </div>
                        <button onclick="deleteVkAccount('${user.id}', '${account.id}')" class="delete-account-btn">
                            <i class="fas fa-trash-alt"></i>
                        </button>
                    </div>
                `).join('') : '';
                
                html += `
                    <div class="user-card">
                        <div class="user-header">
                            <h3><i class="fas fa-user"></i> ${user.username}</h3>
                            <button onclick="deleteUser('${user.id}')" class="delete-user-btn">
                                <i class="fas fa-trash-alt"></i>
                            </button>
                        </div>
                        
                        <div class="api-key-section">
                            <div class="api-key-label">API Ключ:</div>
                            <div class="api-key-container">
                                <code>${user.api_key || 'Не установлен'}</code>
                                <button onclick="copyToClipboard('${user.api_key}')" title="Копировать API ключ">
                                    <i class="fas fa-copy"></i>
                                </button>
                                <button onclick="regenerateApiKey('${user.id}')" title="Перегенерировать API ключ">
                                    <i class="fas fa-sync-alt"></i>
                                </button>
                            </div>
                        </div>
                        
                        <div class="accounts-section">
                            <div class="accounts-header">
                                <h4>Аккаунты Telegram</h4>
                                <button onclick="showTelegramModal('${user.id}')" class="add-account-btn">
                                    <i class="fas fa-plus"></i> Добавить
                                </button>
                            </div>
                            <div class="accounts-list">
                                ${telegramAccountsHtml || '<p>Нет аккаунтов</p>'}
                            </div>
                            
                            <div class="accounts-header">
                                <h4>Аккаунты VK</h4>
                                <button onclick="showVkModal('${user.id}')" class="add-account-btn">
                                    <i class="fas fa-plus"></i> Добавить
                                </button>
                            </div>
                            <div class="accounts-list">
                                ${vkAccountsHtml || '<p>Нет аккаунтов</p>'}
                            </div>
                        </div>
                    </div>
                `;
            });
            
            usersContainer.innerHTML = html;
        } else {
            const error = await response.json();
            usersContainer.innerHTML = `<p>Ошибка: ${error.detail || 'Не удалось загрузить пользователей'}</p>`;
        }
    } catch (error) {
        console.error('Ошибка при загрузке пользователей:', error);
        usersContainer.innerHTML = '<p>Произошла ошибка при загрузке пользователей</p>';
    }
}

// Функция для маскирования токена
function maskToken(token) {
    if (!token) return '';
    if (token.length <= 8) return token;
    return token.substr(0, 4) + '...' + token.substr(-4);
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
    notification.innerHTML = `
        <div class="notification-content">
            <i class="fas ${type === 'success' ? 'fa-check-circle' : type === 'error' ? 'fa-exclamation-circle' : 'fa-info-circle'}"></i>
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
    }, 3000);
    
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
}

function showTelegramModal(userId) {
    currentUser = userId;
    document.getElementById('telegramModal').style.display = 'block';
}

function showVkModal(userId) {
    currentUser = userId;
    document.getElementById('vkModal').style.display = 'block';
}

function closeModal(modalId) {
    document.getElementById(modalId).style.display = 'none';
    
    // Сбрасываем состояние Telegram модального окна
    if (modalId === 'telegramModal') {
        resetTelegramModal();
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

// Функция для сброса модального окна Telegram
function resetTelegramModal() {
    document.getElementById('addTelegramForm').reset();
    document.getElementById('addTelegramForm').style.display = 'block';
    document.getElementById('telegramAuthBlock').style.display = 'none';
    document.getElementById('telegram2FABlock').style.display = 'none';
    document.getElementById('authStatus').textContent = '';
    document.getElementById('twoFAStatus').textContent = '';
    document.getElementById('auth_code').value = '';
    document.getElementById('two_fa_password').value = '';
    currentTelegramAccountId = null;
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