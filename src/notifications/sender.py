import logging

logger = logging.getLogger(__name__)


def send_notification_email(recipient_email: str, task_status: object):
    subject = f"Статус вашей задачи парсинга: {task_status.status}"

    body = f"Здравствуйте!\n\n"
    body += f"Задача парсинга с ID: {task_status.task_id} завершена.\n"
    body += f"Текущий статус: {task_status.status}\n"
    body += f"Прогресс: {task_status.progress}\n"

    if task_status.status == 'COMPLETED':
        body += f"Результаты доступны в файле: {task_status.result_file}\n"
    elif task_status.status == 'FAILED':
        body += f"Произошла ошибка: {task_status.error}\n"

    body += f"\nИсточник: {task_status.source_info.get('source', 'N/A')}\n"
    body += f"Компания: {task_status.source_info.get('company_name', 'N/A')}\n"

    body += "\nСпасибо за использование нашего сервиса!"

    logger.info(f"Attempting to send email to {recipient_email} with subject: '{subject}'")

    # Здесь должна быть реальная логика отправки email.

    try:

        logger.warning(
            f"Email notification for task {task_status.task_id} to {recipient_email} is a placeholder. Real email sending is not implemented.")
        print(f"--- EMAIL NOTIFICATION (SIMULATED) ---")
        print(f"To: {recipient_email}")
        print(f"Subject: {subject}")
        print(f"Body:\n{body}")
        print(f"------------------------------------")

    except Exception as e:
        logger.error(f"Failed to send email notification to {recipient_email}: {e}", exc_info=True)
