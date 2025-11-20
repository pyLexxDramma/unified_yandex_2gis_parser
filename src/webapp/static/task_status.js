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
        
        // Обновляем прогресс-бар с этапами
        updateProgressStages(data.progress || '');
    }
    
    function updateProgressStages(progressText) {
        const stagesContainer = document.getElementById('progress-stages-container');
        if (!stagesContainer) return;
        
        // Показываем контейнер, если задача выполняется (RUNNING или PENDING)
        const statusChip = document.querySelector('.status-chip');
        const taskStatus = statusChip ? statusChip.textContent.trim().toUpperCase() : '';
        
        if (taskStatus === 'RUNNING' || taskStatus === 'PENDING') {
            stagesContainer.style.display = 'block';
        } else {
            stagesContainer.style.display = 'none';
            return;
        }
        
        if (!progressText) progressText = '';
        
        console.log('updateProgressStages called with:', progressText);
        
        // Определяем источник (Yandex или 2GIS)
        let source = null;
        let cleanProgressText = progressText;
        
        if (progressText.match(/^Yandex:/i)) {
            source = 'yandex';
            cleanProgressText = progressText.replace(/^Yandex:\s*/i, '');
            console.log('Detected source: yandex, clean text:', cleanProgressText);
        } else if (progressText.match(/^2GIS:/i)) {
            source = '2gis';
            cleanProgressText = progressText.replace(/^2GIS:\s*/i, '');
            console.log('Detected source: 2gis, clean text:', cleanProgressText);
        }
        
        // Если источник не определен, пробуем определить по task.source_info
        if (!source) {
            // Пробуем определить источник из других данных или показываем оба
            const yandexBlock = document.getElementById('progress-yandex-block');
            const gisBlock = document.getElementById('progress-2gis-block');
            
            // Если оба блока скрыты, значит источник не определен - показываем оба
            if (yandexBlock && gisBlock) {
                const yandexVisible = yandexBlock.style.display !== 'none';
                const gisVisible = gisBlock.style.display !== 'none';
                
                // Если виден только один блок, используем его источник
                if (yandexVisible && !gisVisible) {
                    source = 'yandex';
                } else if (gisVisible && !yandexVisible) {
                    source = '2gis';
                } else {
                    // Если оба видны или оба скрыты, пробуем определить по тексту прогресса
                    if (progressText.toLowerCase().includes('yandex') || progressText.toLowerCase().includes('яндекс')) {
                        source = 'yandex';
                    } else if (progressText.toLowerCase().includes('2gis') || progressText.toLowerCase().includes('2гис')) {
                        source = '2gis';
                    } else {
                        // Если не можем определить, пропускаем обновление
                        return;
                    }
                }
            } else {
                return;
            }
        }
        
        // Определяем текущий этап и прогресс
        let currentStage = null;
        let progressPercent = 0;
        
        // Этап 1: Поиск карточек
        if (cleanProgressText.includes('Поиск карточек') || cleanProgressText.includes('Searching') || cleanProgressText.includes('Finding cards') || cleanProgressText.includes('Processing first page') || cleanProgressText.includes('обработка страницы') || cleanProgressText.includes('Scrolling') || cleanProgressText.includes('прокрутка')) {
            currentStage = 'search';
            // Парсим прогресс из текста (например, "Поиск карточек: найдено 5 карточек" или "обработка страницы 2/5")
            const match = cleanProgressText.match(/(\d+)\s*[\/из]\s*(\d+)/);
            if (match) {
                const current = parseInt(match[1]);
                const total = parseInt(match[2]);
                progressPercent = total > 0 ? Math.round((current / total) * 100) : 0;
                console.log(`Parsed progress from X/Y format: ${current}/${total} = ${progressPercent}%`);
            } else {
                // Пробуем найти число карточек
                const cardsMatch = cleanProgressText.match(/найдено\s+(\d+)/);
                if (cardsMatch) {
                    progressPercent = 50; // Если нашли карточки, значит прогресс ~50%
                    console.log(`Found cards count, setting progress to 50%`);
                } else {
                    progressPercent = 30; // Начало этапа
                    console.log(`No progress found, setting default 30%`);
                }
            }
        }
        // Этап 2: Сканирование карточек
        else if (cleanProgressText.includes('Сканирование карточек') || cleanProgressText.includes('Scanning') || cleanProgressText.includes('Parsing card') || cleanProgressText.match(/\d+\/\d+.*карт/)) {
            currentStage = 'scan';
            const match = cleanProgressText.match(/(\d+)\s*[\/из]\s*(\d+)/);
            if (match) {
                const current = parseInt(match[1]);
                const total = parseInt(match[2]);
                progressPercent = total > 0 ? Math.round((current / total) * 100) : 0;
                console.log(`Parsed scan progress: ${current}/${total} = ${progressPercent}%`);
            } else {
                progressPercent = 50;
                console.log(`No scan progress found, setting default 50%`);
            }
        }
        // Этап 3: Агрегация результатов
        else if (cleanProgressText.includes('Агрегация результатов') || cleanProgressText.includes('Aggregating') || cleanProgressText.includes('completed') || cleanProgressText.includes('завершена')) {
            currentStage = 'aggregate';
            progressPercent = 100;
            console.log(`Aggregation stage, setting 100%`);
        } else {
            console.log('No stage matched, currentStage remains null');
        }
        
        // Обновляем этапы для соответствующего источника
        if (source) {
            // Этап 1: Поиск карточек
            const searchPercent = currentStage === 'search' ? progressPercent : (currentStage && currentStage !== 'search' ? 100 : 0);
            updateStage('search', source, currentStage === 'search', searchPercent);
            
            // Этап 2: Сканирование карточек
            const scanPercent = currentStage === 'scan' ? progressPercent : (currentStage === 'aggregate' ? 100 : (currentStage === 'search' ? 0 : 0));
            updateStage('scan', source, currentStage === 'scan', scanPercent);
            
            // Этап 3: Агрегация результатов
            const aggregatePercent = currentStage === 'aggregate' ? progressPercent : 0;
            updateStage('aggregate', source, currentStage === 'aggregate', aggregatePercent);
            
            console.log(`Progress update: source=${source}, stage=${currentStage}, percent=${progressPercent}`);
        }
    }
    
    function updateStage(stageName, source, isActive, percent) {
        if (!source) return; // Если источник не определен, не обновляем
        
        const suffix = source === 'yandex' ? 'yandex' : '2gis';
        const stageElement = document.getElementById(`stage-${stageName}-${suffix}`);
        const barElement = document.getElementById(`stage-${stageName}-bar-${suffix}`);
        const textElement = document.getElementById(`stage-${stageName}-text-${suffix}`);
        
        if (stageElement && barElement && textElement) {
            // Обновляем классы
            let className = 'stage';
            if (isActive) className += ' active';
            if (percent === 100) className += ' completed';
            stageElement.className = className;
            
            // Обновляем ширину полосы прогресса
            const percentValue = Math.min(100, Math.max(0, percent));
            
            // Принудительно обновляем стили для визуализации
            barElement.style.setProperty('width', `${percentValue}%`, 'important');
            barElement.style.display = 'block';
            barElement.style.height = '100%';
            barElement.style.transition = 'width 0.3s ease';
            
            textElement.textContent = `${percentValue}%`;
            
            console.log(`Updated stage ${stageName} for ${source}: ${percentValue}% (active: ${isActive})`, {
                barWidth: barElement.style.width,
                barDisplay: barElement.style.display,
                elementFound: true
            });
        } else {
            console.warn(`Elements not found for stage ${stageName}-${suffix}:`, {
                stage: !!stageElement,
                bar: !!barElement,
                text: !!textElement,
                stageId: `stage-${stageName}-${suffix}`,
                barId: `stage-${stageName}-bar-${suffix}`,
                textId: `stage-${stageName}-text-${suffix}`
            });
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

