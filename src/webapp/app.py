from __future__ import annotations

import logging
import threading
import os
import uuid
import json
from typing import Dict, Any

from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory
from werkzeug.utils import secure_filename

from src.config.settings import settings
from src.drivers.chrome_driver import ChromeDriver
from src.drivers.pychrome_driver import PychromeDriver
from src.parsers.gis_parser import GisParser
from src.parsers.yandex_parser import YandexParser
from src.storage.csv_writer import CSVWriter

log_level = getattr(settings.log, 'level', 'INFO')
log_format = getattr(settings.log, 'cli_format', '%(asctime)s | %(levelname)-8s | %(message)s')
logging.basicConfig(level=log_level, format=log_format)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'a_very_secret_key_for_development_only')

RESULTS_DIR_NAME = 'results'
results_path = os.path.join(settings.project_root, RESULTS_DIR_NAME)
if not os.path.exists(results_path):
    os.makedirs(results_path, exist_ok=True)
    logger.info(f"Created results directory: {results_path}")
app.config['RESULTS_DIR'] = results_path

active_tasks: Dict[str, Dict[str, Any]] = {}


def run_parser_task(parser_class, url: str, task_id: str) -> None:
    active_tasks[task_id] = {'status': 'running', 'progress': 'Initializing...', 'result_file': None, 'error': None}
    driver = None
    writer = None

    try:
        if parser_class == GisParser:
            driver_instance = PychromeDriver()
            active_tasks[task_id]['progress'] = 'Starting Pychrome driver...'
        elif parser_class == YandexParser:
            driver_instance = ChromeDriver()
            active_tasks[task_id]['progress'] = 'Starting ChromeDriver...'
        else:
            raise ValueError(f"Unknown parser class: {parser_class.__name__}")

        driver_instance.start()
        writer_instance = CSVWriter(settings)
        parser_instance = parser_class(driver=driver_instance, settings=settings)
        parser_instance._url = url

        active_tasks[task_id]['progress'] = f'Starting parsing {parser_class.__name__} for {url}...'

        with writer_instance as w:
            parser_instance.parse(writer=w)

        active_tasks[task_id]['status'] = 'finished'
        active_tasks[task_id]['progress'] = 'Parsing finished successfully.'
        active_tasks[task_id]['result_file'] = os.path.basename(writer_instance._file_path)

    except Exception as e:
        logger.error(f"Error in parser task {task_id}: {e}", exc_info=True)
        active_tasks[task_id]['status'] = 'error'
        active_tasks[task_id]['error'] = str(e)
        active_tasks[task_id]['progress'] = 'An error occurred during parsing.'
    finally:
        if driver:
            driver.stop()


@app.route('/')
def index():
    error_msg = request.args.get('error')
    success_msg = request.args.get('success')
    return render_template('index.html', error=error_msg, success=success_msg)


@app.route('/start_parsing', methods=['POST'])
def start_parsing():
    company_name = request.form.get('company_name')
    company_site = request.form.get('company_site')
    source = request.form.get('source')

    if not company_name or not company_site or not source:
        return redirect(url_for('index', error='Missing required fields.'))

    search_query_encoded = company_name.replace(" ", "+")

    target_url = ""
    parser_class = None

    if source == '2gis':
        target_url = f"https://2gis.ru/search/{search_query_encoded}?maybe_web={company_site}"
        parser_class = GisParser
    elif source == 'yandex':
        target_url = f"https://yandex.ru/maps/?text={search_query_encoded},{company_site}"
        parser_class = YandexParser
    else:
        return redirect(url_for('index', error='Invalid source specified.'))

    if not target_url or not parser_class:
        return redirect(url_for('index', error='Failed to generate target URL or determine parser class.'))

    task_id = str(uuid.uuid4())
    active_tasks[task_id] = {'status': 'pending', 'progress': 'Waiting to start...'}

    thread = threading.Thread(target=run_parser_task, args=(parser_class, target_url, task_id))
    thread.daemon = True
    thread.start()

    return redirect(url_for('task_status', task_id=task_id))


@app.route('/task_status/<task_id>')
def task_status(task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        return render_template('task_status.html', task_id=task_id,
                               task={'status': 'error', 'progress': 'Task not found.', 'error': 'Invalid task ID.'},
                               task_id_str=task_id), 404

    return render_template('task_status.html', task_id=task_id, task=task, task_id_str=task_id)


@app.route('/task_status_api/<task_id>')
def task_status_api(task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    return jsonify(task)


@app.route('/results/<filename>')
def download_results(filename: str):
    results_dir = app.config['RESULTS_DIR']
    secure_filename_safe = secure_filename(filename)
    file_path = os.path.join(results_dir, secure_filename_safe)

    if os.path.exists(file_path):
        return send_from_directory(results_dir, secure_filename_safe, as_attachment=True)
    else:
        logger.warning(f"Requested result file not found: {file_path}")
        return "File not found.", 404


if __name__ == '__main__':
    if not app.config['SECRET_KEY'] or app.config['SECRET_KEY'] == 'a_very_secret_key_for_development_only':
        logger.warning("Using default SECRET_KEY. Please set a strong SECRET_KEY in your environment for production.")
        if not os.environ.get('SECRET_KEY'):
            app.config['SECRET_KEY'] = str(uuid.uuid4())
            logger.info(f"Generated a temporary SECRET_KEY: {app.config['SECRET_KEY']}")

    app.run(host='0.0.0.0', port=5000, debug=True)
