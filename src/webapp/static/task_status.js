// Автоматическое обновление страницы при изменении статуса задачи
(function() {
    'use strict';
    
    // Получаем task_id из URL
    const pathParts = window.location.pathname.split('/');
    const taskId = pathParts[pathParts.length - 1];
    
    if (!taskId || taskId === 'tasks') {
        return; // Не страница задачи
    }
    
    // Проверяем, не было ли уже обновления для этой задачи
    const reloadKey = `task_reloaded_${taskId}`;
    if (sessionStorage.getItem(reloadKey) === 'true') {
        // Уже обновляли, не обновляем снова
        return;
    }
    
    let checkInterval = null;
    let lastStatus = null;
    let hasReloaded = false;
    
    function checkTaskStatus() {
        fetch(`/api/task_status/${taskId}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error('Failed to fetch task status');
                }
                return response.json();
            })
            .then(data => {
                const currentStatus = data.status;
                
                // Если статус изменился на COMPLETED или FAILED, обновляем страницу только один раз
                if ((currentStatus === 'COMPLETED' || currentStatus === 'FAILED') && !hasReloaded) {
                    if (lastStatus !== currentStatus && lastStatus !== null) {
                        // Помечаем, что обновление было выполнено
                        sessionStorage.setItem(reloadKey, 'true');
                        hasReloaded = true;
                        
                        // Останавливаем проверку перед обновлением
                        if (checkInterval) {
                            clearInterval(checkInterval);
                            checkInterval = null;
                        }
                        
                        // Обновляем страницу через небольшую задержку, чтобы пользователь увидел финальный статус
                        setTimeout(() => {
                            window.location.reload();
                        }, 1000);
                        return; // Выходим, чтобы не обновлять lastStatus
                    }
                } else if (currentStatus === 'COMPLETED' || currentStatus === 'FAILED') {
                    // Статус уже COMPLETED/FAILED, останавливаем проверку
                    if (checkInterval) {
                        clearInterval(checkInterval);
                        checkInterval = null;
                    }
                } else {
                    // Если задача еще выполняется, обновляем статус на странице
                    updateStatusOnPage(data);
                }
                
                lastStatus = currentStatus;
            })
            .catch(error => {
                console.error('Error checking task status:', error);
            });
    }
    
    function updateStatusOnPage(data) {
        // Обновляем статус в заголовке
        const statusChip = document.querySelector('.status-chip');
        if (statusChip && data.status) {
            statusChip.textContent = data.status;
            statusChip.className = `status-chip status-${data.status.toLowerCase()}`;
        }
        
        // Обновляем прогресс
        const progressElement = document.querySelector('.task-progress');
        if (progressElement && data.progress) {
            progressElement.textContent = data.progress;
        }
    }
    
    // Начинаем проверку статуса каждые 3 секунды
    if (taskId) {
        checkInterval = setInterval(checkTaskStatus, 3000);
        // Первая проверка сразу
        checkTaskStatus();
    }
    
    // Останавливаем проверку при уходе со страницы
    window.addEventListener('beforeunload', () => {
        if (checkInterval) {
            clearInterval(checkInterval);
        }
    });
})();

