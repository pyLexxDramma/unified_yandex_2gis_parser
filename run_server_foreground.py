#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт для запуска сервера с выводом логов в реальном времени (в текущем терминале)
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

if hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(line_buffering=True, encoding='utf-8')
    except:
        pass

# Импортируем и запускаем uvicorn
if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("Запуск сервера с выводом логов в реальном времени")
    print("Логи будут отображаться в этом терминале")
    print("=" * 60)
    print()
    
    # Настраиваем логирование uvicorn для вывода в реальном времени
    import logging.config
    
    # Создаем простую конфигурацию логирования для uvicorn
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s | %(levelname)-8s | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "access": {
                "format": "%(asctime)s | ACCESS | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
        },
    }
    
    try:
        uvicorn.run(
            "src.webapp.app:app",
            host="0.0.0.0",
            port=8000,
            reload=False,  # Отключаем reload для более стабильного вывода логов
            log_level="info",
            access_log=True,
            use_colors=True,
            log_config=log_config
        )
    except KeyboardInterrupt:
        print("\nСервер остановлен пользователем")
    except Exception as e:
        print(f"\nОшибка при запуске сервера: {e}", file=sys.stderr)
        sys.exit(1)

