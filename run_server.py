#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт для запуска сервера с выводом логов в реальном времени
"""
import sys
import os

# Убеждаемся, что Python работает без буферизации
os.environ['PYTHONUNBUFFERED'] = '1'

# Настраиваем stdout для немедленного вывода
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(line_buffering=True, encoding='utf-8')
    except:
        pass

# Импортируем и запускаем uvicorn
if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("Запуск сервера с выводом логов в реальном времени")
    print("=" * 60)
    print()
    
    uvicorn.run(
        "src.webapp.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        access_log=True
    )

