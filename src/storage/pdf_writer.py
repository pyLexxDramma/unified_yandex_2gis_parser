from __future__ import annotations
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

logger = logging.getLogger(__name__)


class PDFWriter:
    """Генератор PDF отчетов"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.doc = None
        self.story = []
        self.styles = getSampleStyleSheet()
        
        # Настраиваем стили
        self._setup_styles()
    
    def _setup_styles(self):
        """Настраивает стили для PDF"""
        # Заголовок
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1976d2'),
            spaceAfter=30,
            alignment=TA_CENTER
        ))
        
        # Подзаголовок
        self.styles.add(ParagraphStyle(
            name='CustomHeading2',
            parent=self.styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#424242'),
            spaceAfter=12,
            spaceBefore=20
        ))
        
        # Обычный текст
        self.styles.add(ParagraphStyle(
            name='CustomBody',
            parent=self.styles['Normal'],
            fontSize=10,
            spaceAfter=6
        ))
        
        # Метаданные
        self.styles.add(ParagraphStyle(
            name='CustomMeta',
            parent=self.styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#757575'),
            spaceAfter=3
        ))
    
    def generate_report(self, task_data: Dict[str, Any], statistics: Dict[str, Any], 
                       cards: List[Dict[str, Any]], source_info: Dict[str, Any]) -> str:
        """Генерирует PDF отчет"""
        try:
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            
            self.doc = SimpleDocTemplate(
                self.file_path,
                pagesize=A4,
                rightMargin=2*cm,
                leftMargin=2*cm,
                topMargin=2*cm,
                bottomMargin=2*cm
            )
            
            self.story = []
            
            # Заголовок
            company_name = source_info.get('company_name', 'Неизвестная компания')
            self.story.append(Paragraph(f"Отчет по компании: {company_name}", self.styles['CustomTitle']))
            self.story.append(Spacer(1, 0.5*cm))
            
            # Метаданные
            current_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            self.story.append(Paragraph(f"Дата создания: {current_time}", self.styles['CustomMeta']))
            self.story.append(Paragraph(f"Источник: {source_info.get('source', 'N/A').upper()}", self.styles['CustomMeta']))
            self.story.append(Spacer(1, 0.5*cm))
            
            # Общая статистика
            self._add_statistics_section(statistics, source_info)
            
            # Детали по карточкам
            if cards:
                self._add_cards_section(cards)
            
            # Генерируем PDF
            self.doc.build(self.story)
            logger.info(f"PDF report generated: {self.file_path}")
            return self.file_path
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {e}", exc_info=True)
            raise
    
    def _add_statistics_section(self, statistics: Dict[str, Any], source_info: Dict[str, Any]):
        """Добавляет секцию со статистикой"""
        self.story.append(Paragraph("Общая статистика", self.styles['CustomHeading2']))
        
        # Если есть данные от обоих источников
        if statistics.get('sources'):
            # Общие результаты
            self.story.append(Paragraph("Общие результаты (Яндекс + 2GIS)", self.styles['Heading3']))
            self._add_statistics_table(statistics, source_info)
            
            # Результаты по источникам
            if statistics.get('sources', {}).get('yandex'):
                self.story.append(Spacer(1, 0.3*cm))
                self.story.append(Paragraph("Яндекс.Карты", self.styles['Heading3']))
                self._add_statistics_table(statistics['sources']['yandex'], source_info, is_yandex=True)
            
            if statistics.get('sources', {}).get('2gis'):
                self.story.append(Spacer(1, 0.3*cm))
                self.story.append(Paragraph("2GIS", self.styles['Heading3']))
                self._add_statistics_table(statistics['sources']['2gis'], source_info, is_yandex=False)
        else:
            # Один источник
            is_yandex = source_info.get('source', '').lower() == 'yandex'
            self._add_statistics_table(statistics, source_info, is_yandex=is_yandex)
        
        self.story.append(Spacer(1, 0.5*cm))
    
    def _add_statistics_table(self, stats: Dict[str, Any], source_info: Dict[str, Any], is_yandex: bool = True):
        """Добавляет таблицу со статистикой"""
        data = [
            ['Метрика', 'Значение'],
            ['Карточек найдено', str(stats.get('total_cards_found', 0))],
            ['Средний рейтинг', f"{stats.get('aggregated_rating', 0):.2f}" if stats.get('aggregated_rating') else "—"],
            ['Всего отзывов', str(stats.get('aggregated_reviews_count', 0))],
            ['Отвечено отзывов', str(stats.get('aggregated_answered_reviews_count', 0))],
        ]
        
        # Процент отзывов с ответами
        total_reviews = stats.get('aggregated_reviews_count', 0)
        answered_reviews = stats.get('aggregated_answered_reviews_count', 0)
        if total_reviews > 0:
            percent = (answered_reviews / total_reviews) * 100
            data.append(['Процент отзывов с ответами', f"{percent:.2f}%"])
        else:
            data.append(['Процент отзывов с ответами', "0%"])
        
        # Среднее время ответа
        avg_time = stats.get('aggregated_avg_response_time', 0)
        if avg_time:
            time_unit = "дней" if is_yandex else "месяцев"
            data.append(['Среднее время ответа', f"{avg_time:.2f} {time_unit}"])
        else:
            data.append(['Среднее время ответа', "—"])
        
        data.append(['Положительных отзывов (4-5⭐)', str(stats.get('aggregated_positive_reviews', 0))])
        data.append(['Отрицательных отзывов (1-3⭐)', str(stats.get('aggregated_negative_reviews', 0))])
        
        table = Table(data, colWidths=[8*cm, 6*cm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1976d2')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
        ]))
        
        self.story.append(table)
    
    def _add_cards_section(self, cards: List[Dict[str, Any]]):
        """Добавляет секцию с деталями по карточкам"""
        self.story.append(PageBreak())
        self.story.append(Paragraph("Детали по карточкам", self.styles['CustomHeading2']))
        self.story.append(Spacer(1, 0.3*cm))
        
        for idx, card in enumerate(cards, 1):
            self.story.append(Paragraph(f"Карточка {idx}: {card.get('card_name', 'Без названия')}", self.styles['Heading3']))
            
            # Информация о карточке
            card_data = [
                ['Параметр', 'Значение'],
                ['Адрес', card.get('card_address', 'Не указан')],
                ['Телефон', card.get('card_phone', 'Не указан')],
                ['Рейтинг', str(card.get('card_rating', '—'))],
                ['Количество отзывов', str(card.get('card_reviews_count', 0))],
            ]
            
            if card.get('card_answered_reviews_count') is not None:
                card_data.append(['Отвечено отзывов', str(card.get('card_answered_reviews_count', 0))])
                card_data.append(['Не отвечено отзывов', str(card.get('card_unanswered_reviews_count', 0))])
            
            if card.get('card_avg_response_time'):
                time_unit = "дней" if card.get('source') == 'yandex' else "месяцев"
                card_data.append(['Среднее время ответа', f"{card.get('card_avg_response_time')} {time_unit}"])
            
            card_data.append(['Положительных отзывов', str(card.get('card_reviews_positive', 0))])
            card_data.append(['Отрицательных отзывов', str(card.get('card_reviews_negative', 0))])
            
            if card.get('source'):
                source_name = "Яндекс.Карты" if card.get('source') == 'yandex' else "2GIS"
                card_data.append(['Источник', source_name])
            
            table = Table(card_data, colWidths=[6*cm, 8*cm])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
            ]))
            
            self.story.append(table)
            self.story.append(Spacer(1, 0.3*cm))
            
            # Отзывы
            reviews = card.get('detailed_reviews', [])
            if reviews:
                self.story.append(Paragraph("Отзывы:", self.styles['Heading4']))
                for review_idx, review in enumerate(reviews[:10], 1):  # Ограничиваем 10 отзывами на карточку
                    review_text = f"<b>Отзыв {review_idx}</b><br/>"
                    if review.get('review_author'):
                        review_text += f"Автор: {review.get('review_author')}<br/>"
                    if review.get('review_rating'):
                        review_text += f"Рейтинг: {'⭐' * int(review.get('review_rating', 0))} ({review.get('review_rating')})<br/>"
                    if review.get('review_date'):
                        review_text += f"Дата: {review.get('review_date')}<br/>"
                    if review.get('review_text'):
                        review_text += f"Текст: {review.get('review_text')[:200]}{'...' if len(review.get('review_text', '')) > 200 else ''}"
                    
                    self.story.append(Paragraph(review_text, self.styles['CustomBody']))
                    self.story.append(Spacer(1, 0.2*cm))
                
                if len(reviews) > 10:
                    self.story.append(Paragraph(f"... и еще {len(reviews) - 10} отзывов", self.styles['CustomMeta']))
            
            self.story.append(Spacer(1, 0.5*cm))
            
            # Разрыв страницы после каждых 2 карточек
            if idx % 2 == 0 and idx < len(cards):
                self.story.append(PageBreak())

