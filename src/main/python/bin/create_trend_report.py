import alblogs
import logging
import datetime
import urllib.parse

import matplotlib.pyplot as plt
import numpy
import os
import tempfile

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm, mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_LEFT, TA_CENTER
from reportlab.pdfgen import canvas

LOGGER = None

DEFAULT_FONT = "Helvetica"
DEFAULT_FONT_BOLD = "Helvetica-Bold"

REQUEST_TYPE_MAP = {"http": "HTTP", "https": "HTTP SSL/TLS", "h2": "HTTP/2 SSL/TLS", "ws": "Websockets", "wss": "Websockets SSL/TLS"}


class FooterCanvas(canvas.Canvas):

    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self.pages = []

    def showPage(self):
        self.pages.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        page_count = len(self.pages)
        for page in self.pages:
            self.__dict__.update(page)
            self.draw_canvas(page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def _draw_header(self):
        self.setStrokeColorRGB(0, 0, 0)
        self.setLineWidth(0.5)
        self.line(1*cm, A4[0] - 1*cm, A4[1] - (1*cm), A4[0] - 1*cm)
        self.setFont(DEFAULT_FONT, 10)
        self.drawString(1 * cm, A4[0] - (0.90*cm), "Request Performance Trends")
        # self.drawRightString(A4[1] - (1*cm), A4[0] - (0.90*cm), "Logs for date: {}".format(args.date))

    def _draw_footer(self, page_count):
        page = "Page %s of %s" % (self._pageNumber, page_count)
        self.setStrokeColorRGB(0, 0, 0)
        self.setLineWidth(0.5)
        self.line(1*cm, 1*cm, A4[1] - (1*cm), 1*cm)
        self.setFont(DEFAULT_FONT, 10)
        self.drawRightString(A4[1] - (1*cm), (0.65*cm), page)

    def draw_canvas(self, page_count):
        self.saveState()
        self._draw_header()
        self._draw_footer(page_count)
        self.restoreState()


def read_arguments():
    argparser = alblogs.get_default_argparser()
    argparser.add_argument("-f", "--fromdate", action='store', metavar="FROM", type=str, help="Start date for report (inclusive)")
    argparser.add_argument("-t", "--todate", action='store', metavar="TO", type=str, help="End date for report (inclusive)")

    args = argparser.parse_args()

    # try to convert date arguments to check if it is a valid date
    if not (args.fromdate is None):
        datetime.datetime.strptime(args.fromdate, '%Y-%m-%d')
    if not (args.todate is None):
        datetime.datetime.strptime(args.todate, '%Y-%m-%d')

    return args


def fill_top_10_trend_gaps_for_url(statsdb, result, url):
    dates = sorted(result.keys())
    for enum_date in dates:
        if result[enum_date].get(url) is None:
            LOGGER.debug("Missing: date = {} | url = {}".format(enum_date, url))
            rows = statsdb.query_url_stats_for_url_and_date(url, enum_date)
            row = rows.fetchone()
            if row is None:
                LOGGER.debug("Url {} not found for date {}".format(url, enum_date))
            else:
                result[enum_date][url] = {"url": row["url"],
                                          "found": True,
                                          "sum_request_count": row["sum_request_count"],
                                          "sum_target_processing_time": row["sum_target_processing_time"],
                                          "avg_target_processing_time": row["sum_target_processing_time"] / row["sum_request_count"]
                                          }


def fill_top_10_trend_gaps(statsdb, result, dates):
    for url in result.keys():
        url_stats = result[url]
        for datestr in dates:
            date_stats = url_stats["dates"].get(datestr)
            if date_stats is None:
                LOGGER.debug("Missing: {} {}".format(datestr, url))
                rows = statsdb.query_url_stats_for_url_and_date(url, datestr)
                row = rows.fetchone()
                if row is None:
                    LOGGER.debug("No data found for {} {}".format(datestr, url))
                else:
                    avg_processing_time = row["sum_target_processing_time"] / row["sum_request_count"]
                    date_stats = {"date": datestr,
                                  "pos": 0,
                                  "sum_request_count": row['sum_request_count'],
                                  "sum_processing_time": row['sum_target_processing_time'],
                                  "avg_processing_time": avg_processing_time
                                 }
                    url_stats["dates"][datestr] = date_stats
                    url_stats["min_avg_processing_time"] = min(url_stats["min_avg_processing_time"], avg_processing_time)
                    url_stats["max_avg_processing_time"] = max(url_stats["max_avg_processing_time"], avg_processing_time)


def fill_top_10_trend_gaps_excl_5xx(statsdb, result, dates):
    for url in result.keys():
        url_stats = result[url]
        for datestr in dates:
            date_stats = url_stats["dates"].get(datestr)
            if date_stats is None:
                LOGGER.debug("Missing: {} {}".format(datestr, url))
                rows = statsdb.query_url_stats_for_url_and_date_excl_5xx(url, datestr)
                row = rows.fetchone()
                if row is None:
                    LOGGER.debug("No data found for {} {}".format(datestr, url))
                else:
                    avg_processing_time = row["sum_target_processing_time"] / row["sum_request_count"]
                    date_stats = {"date": datestr,
                                  "pos": 0,
                                  "sum_request_count": row['sum_request_count'],
                                  "sum_processing_time": row['sum_target_processing_time'],
                                  "avg_processing_time": avg_processing_time
                                 }
                    url_stats["dates"][datestr] = date_stats
                    url_stats["min_avg_processing_time"] = min(url_stats["min_avg_processing_time"], avg_processing_time)
                    url_stats["max_avg_processing_time"] = max(url_stats["max_avg_processing_time"], avg_processing_time)


def query_top_10_trend_data(statsdb, from_date, to_date, exclude_dates):
    result = {}
    dates = []
    day_delta = datetime.timedelta(days=1)
    enum_date = from_date

    while enum_date <= to_date:
        datestr = enum_date
        if datestr not in exclude_dates:
            dates.append(datestr)
            rows = statsdb.query_top_x_url_by_time(datestr)
            pos = 1
            for row in rows:
                avg_processing_time = row["sum_target_processing_time"] / row["sum_request_count"]
                url_stats = result.get(row['url'])
                if url_stats is None:
                    url_stats = {"url": row['url'], "dates": {}, "min_avg_processing_time": avg_processing_time, "max_avg_processing_time": avg_processing_time}
                    result[row['url']] = url_stats

                # There are no date_stats for this date, as this is the initial load
                date_stats = {"date": datestr,
                              "pos": pos,
                              "sum_request_count": row['sum_request_count'],
                              "sum_processing_time": row['sum_target_processing_time'],
                              "avg_processing_time": avg_processing_time
                             }
                url_stats["min_avg_processing_time"] = min(url_stats["min_avg_processing_time"], avg_processing_time)
                url_stats["max_avg_processing_time"] = max(url_stats["max_avg_processing_time"], avg_processing_time)

                url_stats["dates"][datestr] = date_stats
                pos += 1

        enum_date = (datetime.datetime.strptime(enum_date, '%Y-%m-%d') + day_delta).strftime('%Y-%m-%d')

    fill_top_10_trend_gaps(statsdb, result, dates)

    return result, dates


def query_top_10_trend_data_excl_5xx(statsdb, from_date, to_date, exclude_dates):
    result = {}
    dates = []
    day_delta = datetime.timedelta(days=1)
    enum_date = from_date

    while enum_date <= to_date:
        datestr = enum_date
        if datestr not in exclude_dates:
            dates.append(datestr)
            rows = statsdb.query_top_x_url_by_time_excl_5xx(datestr)
            pos = 1
            for row in rows:
                avg_processing_time = row["sum_target_processing_time"] / row["sum_request_count"]
                url_stats = result.get(row['url'])
                if url_stats is None:
                    url_stats = {"url": row['url'], "dates": {}, "min_avg_processing_time": avg_processing_time, "max_avg_processing_time": avg_processing_time}
                    result[row['url']] = url_stats

                # There are no date_stats for this date, as this is the initial load
                date_stats = {"date": datestr,
                              "pos": pos,
                              "sum_request_count": row['sum_request_count'],
                              "sum_processing_time": row['sum_target_processing_time'],
                              "avg_processing_time": avg_processing_time
                             }
                url_stats["min_avg_processing_time"] = min(url_stats["min_avg_processing_time"], avg_processing_time)
                url_stats["max_avg_processing_time"] = max(url_stats["max_avg_processing_time"], avg_processing_time)

                url_stats["dates"][datestr] = date_stats
                pos += 1

        enum_date = (datetime.datetime.strptime(enum_date, '%Y-%m-%d') + day_delta).strftime('%Y-%m-%d')

    fill_top_10_trend_gaps_excl_5xx(statsdb, result, dates)

    return result, dates


def query_request_volume_data(statsdb, from_date, to_date, exclude_dates):
    dates = []
    result = {}
    day_delta = datetime.timedelta(days=1)
    enum_date = from_date

    while enum_date <= to_date:
        datestr = enum_date
        if datestr not in exclude_dates:
            dates.append(datestr)
            rows = statsdb.query_day_totals(datestr)
            row = rows.fetchone()
            result[datestr] = {"count": row["request_count"], "time": row["target_processing_time_sec"]}

        enum_date = (datetime.datetime.strptime(enum_date, '%Y-%m-%d') + day_delta).strftime('%Y-%m-%d')

    return dates, result


def define_paragraph_styles():
    styles = {}

    styles["table_title"] = ParagraphStyle('table_title', fontName=DEFAULT_FONT, fontSize=16, textColor=colors.black, alignment=TA_CENTER)
    styles["table_subtitle"] = ParagraphStyle('table_title', fontName=DEFAULT_FONT, fontSize=12, textColor=colors.black, alignment=TA_CENTER)
    styles["table_header_left"] = ParagraphStyle('table_header_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.white, alignment=TA_LEFT)
    styles["table_header_center"] = ParagraphStyle('table_header_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.white, alignment=TA_CENTER)
    styles["table_header_right"] = ParagraphStyle('table_header_right', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.white, alignment=TA_RIGHT)
    styles["table_data_left"] = ParagraphStyle('table_data_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.white, alignment=TA_LEFT)
    styles["table_data_left_green"] = ParagraphStyle('table_data_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.green, alignment=TA_LEFT)
    styles["table_data_left_orange"] = ParagraphStyle('table_data_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.orange, alignment=TA_LEFT)
    styles["table_data_left_red"] = ParagraphStyle('table_data_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.red, alignment=TA_LEFT)
    styles["table_data_left_yellow"] = ParagraphStyle('table_data_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.yellow, alignment=TA_LEFT)
    styles["table_data_right"] = ParagraphStyle('table_data_right', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.white, alignment=TA_RIGHT)
    styles["table_data_right_bold"] = ParagraphStyle('table_data_right', fontName=DEFAULT_FONT_BOLD, fontSize=8, textColor=colors.black, backColor=colors.white, alignment=TA_RIGHT)
    styles["table_data_right_green"] = ParagraphStyle('table_data_right', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.green, alignment=TA_RIGHT)
    styles["table_data_right_orange"] = ParagraphStyle('table_data_right', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.orange, alignment=TA_RIGHT)
    styles["table_data_right_red"] = ParagraphStyle('table_data_right', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.red, alignment=TA_RIGHT)
    styles["table_data_right_yellow"] = ParagraphStyle('table_data_right', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.yellow, alignment=TA_RIGHT)

    return styles


def define_trend_table_style(span_groups, color_groups):
    table_style = TableStyle()

    # All rows
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")

    table_style.add('INNERGRID', (0, 3), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 3), (-1, -1), 0.4 * mm, colors.black)

    # Header row
    # table_style.add('INNERGRID', (0, 0), (-1, 0), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, 2), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (-1, 2), colors.black)
    table_style.add('ALIGN', (0, 0), (-1, 0), "CENTER")

    for span_group in span_groups:
        table_style.add('SPAN', (span_group["left"], span_group["top"]), (span_group["right"], span_group["bottom"]))

    for color_group in color_groups:
        table_style.add("BACKGROUND", (color_group["col"], color_group["row"]), (color_group["col"], color_group["row"]), color_group["color"])
    return table_style


def define_request_type_table_style(span_groups, color_groups):
    table_style = TableStyle()

    # All rows
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")

    table_style.add('INNERGRID', (0, 3), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 3), (-1, -1), 0.4 * mm, colors.black)

    # Header row
    # table_style.add('INNERGRID', (0, 0), (-1, 0), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, 2), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (-1, 2), colors.black)
    table_style.add('ALIGN', (0, 0), (-1, 0), "CENTER")

    table_style.add('BOX', (0, -1), (-1, -2), 0.4 * mm, colors.black)

    for span_group in span_groups:
        table_style.add('SPAN', (span_group["left"], span_group["top"]), (span_group["right"], span_group["bottom"]))

    for color_group in color_groups:
        table_style.add("BACKGROUND", (color_group["col"], color_group["row"]), (color_group["col"], color_group["row"]), color_group["color"])
    return table_style


def define_url_table_style():
    table_style = TableStyle()

    # All rows
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")

    table_style.add('INNERGRID', (0, 1), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 1), (-1, -1), 0.4 * mm, colors.black)

    # Header row
    table_style.add('INNERGRID', (0, 0), (-1, 0), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, 0), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (-1, 0), colors.black)
    table_style.add('ALIGN', (0, 0), (-1, 0), "CENTER")

    return table_style


def define_trend_legend_table_style():
    table_style = TableStyle()

    # All rows
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")

    table_style.add('INNERGRID', (0, 0), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, -1), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (0, 0), colors.green)
    table_style.add('BACKGROUND', (0, 1), (0, 1), colors.red)
    table_style.add('BACKGROUND', (0, 2), (0, 2), colors.yellow)
    table_style.add('BACKGROUND', (0, 3), (0, 3), colors.orange)

    return table_style


def generate_trend_table(styles, trend_data, dates):
    span_groups = []
    cur_span_group = None

    color_groups = []

    table_data = []

    header_row1 = []
    header_row2 = []
    header_row3 = []
    header_row1.append(Paragraph("", style=styles["table_header_center"]))
    header_row2.append(Paragraph("", style=styles["table_header_center"]))
    header_row3.append(Paragraph("URL Path", style=styles["table_header_center"]))

    prev_month_part = ""

    widths = [9 * cm]

    colnum = 1
    for datestr in dates:
        widths.append(1.4 * cm)
        month_part = datestr[:7]
        if month_part != prev_month_part:
            if cur_span_group is not None:
                cur_span_group["right"] = colnum - 1
            cur_span_group = {"left": colnum, "top": 0, "bottom": 0}
            span_groups.append(cur_span_group)
            prev_month_part = month_part
            header_row1.append(Paragraph(month_part, style=styles["table_header_left"]))
        else:
            header_row1.append(Paragraph("", style=styles["table_header_center"]))

        header_row2.append(Paragraph(datestr[8:11], style=styles["table_header_center"]))
        daycode = datetime.datetime.strptime(datestr, "%Y-%m-%d").strftime("%a")
        header_row3.append(Paragraph(daycode, style=styles["table_header_center"]))

        colnum += 1

    cur_span_group["right"] = colnum - 1

    header_row1.append("")
    header_row2.append("")
    header_row3.append("")
    widths.append(0.2 * cm)
    colnum += 1

    color_groups.append({"row": 0, "col": colnum - 1, "color": colors.white})
    color_groups.append({"row": 1, "col": colnum - 1, "color": colors.white})
    color_groups.append({"row": 2, "col": colnum - 1, "color": colors.white})

    cur_span_group = {"left": colnum, "top": 1, "bottom": 1}
    span_groups.append(cur_span_group)

    header_row1.append("")
    header_row2.append(Paragraph("Improvement", style=styles["table_header_center"]))
    header_row3.append(Paragraph("last/first", style=styles["table_header_left"]))
    widths.append(1.5 * cm)
    colnum += 1

    header_row1.append("")
    header_row2.append(Paragraph("", style=styles["table_header_center"]))
    header_row3.append(Paragraph("max/min", style=styles["table_header_center"]))
    widths.append(1.5 * cm)
    colnum += 1

    cur_span_group["right"] = colnum - 1

    table_data.append(header_row1)
    table_data.append(header_row2)
    table_data.append(header_row3)

    urls = sorted(trend_data.keys())
    row_num = len(table_data) - 1
    for url in urls:
        row_num += 1
        display_url = urllib.parse.urlparse(url).path
        url_stats = trend_data[url]
        data_row = []
        data_row.append(Paragraph(display_url, style=styles["table_data_left"]))
        col_num = 0
        prev_value = None
        first_value = None
        value = 0

        for datestr in dates:
            col_num += 1
            date_stats = url_stats["dates"].get(datestr)
            if date_stats is None:
                data_row.append(Paragraph("-", style=styles["table_data_right"]))
            else:
                value = date_stats["avg_processing_time"]
                pstyle = "table_data_right"

                if first_value is None:
                    first_value = value

                if value == url_stats["max_avg_processing_time"]:
                    color_groups.append({"row": row_num, "col": col_num, "color": colors.red})
                    pstyle = "table_data_right_red"
                elif value == url_stats["min_avg_processing_time"]:
                    color_groups.append({"row": row_num, "col": col_num, "color": colors.green})
                    pstyle = "table_data_right_green"
                else:
                    if prev_value is not None:
                        if value > prev_value:
                            color_groups.append({"row": row_num, "col": col_num, "color": colors.orange})
                            pstyle = "table_data_right_orange"
                        elif value < prev_value:
                            if value == url_stats["min_avg_processing_time"]:
                                color_groups.append({"row": row_num, "col": col_num, "color": colors.green})
                                pstyle = "table_data_right_green"
                            else:
                                color_groups.append({"row": row_num, "col": col_num, "color": colors.yellow})
                                pstyle = "table_data_right_yellow"

                data_row.append(Paragraph("{:0,.3f}".format(value), style=styles[pstyle]))

                prev_value = value

        data_row.append("")
        widths.append(0.2 * cm)

        first_last = (first_value/value)
        if first_last < 1:
            data_row.append(Paragraph("{:0,.1f}".format(first_last), style=styles["table_data_right_red"]))
            color_groups.append({"row": row_num, "col": len(data_row)-1, "color": colors.red})
        else:
            data_row.append(Paragraph("{:0,.1f}".format(first_last), style=styles["table_data_right_green"]))
            color_groups.append({"row": row_num, "col": len(data_row)-1, "color": colors.green})

        max_min = (url_stats["max_avg_processing_time"]/url_stats["min_avg_processing_time"])
        if max_min < 1:
            data_row.append(Paragraph("{:0,.1f}".format(max_min), style=styles["table_data_right_red"]))
            color_groups.append({"row": row_num, "col": len(data_row)-1, "color": colors.red})
        else:
            data_row.append(Paragraph("{:0,.1f}".format(max_min), style=styles["table_data_right_green"]))
            color_groups.append({"row": row_num, "col": len(data_row)-1, "color": colors.green})

        table_data.append(data_row)

    t = Table(table_data, colWidths=widths)
    t.setStyle(define_trend_table_style(span_groups, color_groups))

    return t


def create_trend_legend_row(label, text, style,  styles):
    result = []
    result.append(Paragraph(label, style=style))
    result.append(Paragraph(text, style=styles["table_data_left"]))
    return result


def generate_trend_legend_table(styles):
    table_data = []
    table_data.append(create_trend_legend_row("Green", "Fastest response time", styles["table_data_left_green"], styles))
    table_data.append(create_trend_legend_row("Red", "Slowest response time", styles["table_data_left_red"], styles))
    table_data.append(create_trend_legend_row("Yellow", "Faster than previous day, not the fastest overall", styles["table_data_left_yellow"], styles))
    table_data.append(create_trend_legend_row("Orange", "Slower than previous day, not the slowest overall", styles["table_data_left_orange"], styles))

    t = Table(table_data, colWidths=[2 * cm, 7 * cm])
    t.setStyle(define_trend_legend_table_style())

    return t


def generate_url_table(styles, url_stats, dates):
    table_data = []
    widths = []

    header_row = []
    header_row.append(Paragraph("Date", style=styles["table_header_left"]))
    widths.append(2 * cm)

    header_row.append(Paragraph("Requests", style=styles["table_header_left"]))
    widths.append(1.7 * cm)

    header_row.append(Paragraph("Time", style=styles["table_header_left"]))
    widths.append(1.7 * cm)

    header_row.append(Paragraph("Avg", style=styles["table_header_left"]))
    widths.append(1.7 * cm)

    header_row.append(Paragraph("Top 10", style=styles["table_header_left"]))
    widths.append(1.7 * cm)

    table_data.append(header_row)

    for datestr in dates:
        date_stat = url_stats["dates"].get(datestr)
        data_row = []
        data_row.append(Paragraph("{}".format(datestr), style=styles["table_data_left"]))
        if date_stat is None:
            data_row.append(Paragraph("-", style=styles["table_data_right"]))
            data_row.append(Paragraph("-", style=styles["table_data_right"]))
            data_row.append(Paragraph("-", style=styles["table_data_right"]))
            data_row.append(Paragraph("-", style=styles["table_data_right"]))
        else:
            data_row.append(Paragraph("{:,d}".format(date_stat["sum_request_count"]), style=styles["table_data_right"]))
            data_row.append(Paragraph("{:0,.1f}".format(date_stat["sum_processing_time"]), style=styles["table_data_right"]))
            data_row.append(Paragraph("{:0,.3f}".format(date_stat["avg_processing_time"]), style=styles["table_data_right"]))
            data_row.append(Paragraph("{:,d}".format(date_stat["pos"]), style=styles["table_data_right"]))

        table_data.append(data_row)

    t = Table(table_data, colWidths=widths)
    t.setStyle(define_url_table_style())

    return t


def generate_date_time_chart(url_stats, dates):

    fig = plt.figure(figsize=(7,4))

    fig.set_constrained_layout({"h_pad": 0.25, "w_pad": 3.0/72.0})

    ax = fig.add_subplot(111)

    values = []
    for datestr in dates:
        date_stat = url_stats["dates"].get(datestr)
        if date_stat is None:
            values.append(None)
        else:
            values.append(date_stat["sum_processing_time"])

    ax.plot(dates, values, label=url_stats["url"])

    plt.xticks(dates, rotation=270)
    plt.xlabel('Date')

    plt.ylabel('Seconds')
    plt.title('Total request time per day')

    ax.grid(True)

    figfile = tempfile.NamedTemporaryFile(suffix=".png", dir=config.get_temp_dir(), delete=False)
    plt.savefig(figfile)
    filename = figfile.name
    figfile.close()

    return filename


def generate_date_count_chart(url_stats, dates):

    fig = plt.figure(figsize=(7,4))

    fig.set_constrained_layout({"h_pad": 0.25, "w_pad": 3.0/72.0})

    ax = fig.add_subplot(111)

    values = []
    for datestr in dates:
        date_stat = url_stats["dates"].get(datestr)
        if date_stat is None:
            values.append(None)
        else:
            values.append(date_stat["sum_request_count"])

    ax.plot(dates, values, label=url_stats["url"])

    plt.xticks(dates, rotation=270)
    plt.xlabel('Date')

    plt.ylabel('Count')
    plt.title('Requests per day')

    ax.grid(True)

    figfile = tempfile.NamedTemporaryFile(suffix=".png", dir=config.get_temp_dir(), delete=False)
    plt.savefig(figfile)
    filename = figfile.name
    figfile.close()

    return filename


def define_page_table_style():
    table_style = TableStyle()
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")
    table_style.add('ALIGN', (0, 0), (-1, -1), "CENTER")

    table_style.add('SPAN', (0, 0), (0, 1))

    return table_style


def define_page_table_style_no_span():
    table_style = TableStyle()
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")
    table_style.add('ALIGN', (0, 0), (-1, -1), "CENTER")

    return table_style


def define_request_volume_table_style():
    table_style = TableStyle()

    # All rows
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")

    table_style.add('INNERGRID', (0, 1), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 1), (-1, -1), 0.4 * mm, colors.black)

    # Header row
    table_style.add('INNERGRID', (0, 0), (-1, 0), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, 0), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (-1, 0), colors.black)
    table_style.add('ALIGN', (0, 0), (-1, 0), "CENTER")

    return table_style


def generate_request_volume_table(styles, data, dates):
    table_data = []

    header_row = []
    header_row.append(Paragraph("Date", styles["table_header_center"]))
    header_row.append(Paragraph("Count", styles["table_header_center"]))
    header_row.append(Paragraph("Time", styles["table_header_center"]))
    header_row.append(Paragraph("Average", styles["table_header_center"]))
    table_data.append(header_row)

    for datestr in dates:
        data_row = []

        data_row.append(Paragraph("{}".format(datestr), styles["table_data_left"]))
        data_row.append(Paragraph("{:,d}".format(data[datestr]["count"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:,.0f}".format(data[datestr]["time"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:,.3f}".format(data[datestr]["time"]/data[datestr]["count"]), styles["table_data_right"]))
        table_data.append(data_row)

    t = Table(table_data, colWidths=[2*cm, 2*cm, 2*cm, 2*cm])
    t.setStyle(define_request_volume_table_style())

    return t


def generate_request_volume_count_chart(data, dates):
    fig = plt.figure(figsize=(7,4))

    fig.set_constrained_layout({"h_pad": 0.25, "w_pad": 3.0/72.0})

    ax = fig.add_subplot(111)

    axis_data = []
    for datestr in dates:
        axis_data.append(data[datestr]["count"])

    ax.plot(dates, axis_data)

    plt.xticks(dates, rotation=270)
    plt.xlabel('Date')

    plt.ylabel('Count')
    plt.title('Requests per day')

    ax.grid(True)
    ax.set_ylim(ymin=0)

    figfile = tempfile.NamedTemporaryFile(suffix=".png", dir=config.get_temp_dir(), delete=False)
    plt.savefig(figfile)
    filename = figfile.name
    figfile.close()

    return filename


def generate_request_volume_time_chart(data, dates):
    fig = plt.figure(figsize=(7,4))

    fig.set_constrained_layout({"h_pad": 0.25, "w_pad": 3.0/72.0})

    ax = fig.add_subplot(111)

    axis_data = []
    for datestr in dates:
        axis_data.append(data[datestr]["time"])

    ax.plot(dates, axis_data)

    plt.xticks(dates, rotation=270)
    plt.xlabel('Date')

    plt.ylabel('Seconds')
    plt.title('Total request time per day')

    ax.grid(True)
    ax.set_ylim(ymin=0)

    figfile = tempfile.NamedTemporaryFile(suffix=".png", dir=config.get_temp_dir(), delete=False)
    plt.savefig(figfile)
    filename = figfile.name
    figfile.close()

    return filename


def generate_request_volume_avg_chart(data, dates):
    fig = plt.figure(figsize=(7,4))

    fig.set_constrained_layout({"h_pad": 0.25, "w_pad": 3.0/72.0})

    ax = fig.add_subplot(111)

    axis_data = []
    for datestr in dates:
        axis_data.append(data[datestr]["time"] / data[datestr]["count"])

    ax.plot(dates, axis_data)

    plt.xticks(dates, rotation=270)
    plt.xlabel('Date')

    plt.ylabel('Seconds')
    plt.title('Average processing time per day')

    ax.grid(True)
    ax.set_ylim(ymin=0)

    figfile = tempfile.NamedTemporaryFile(suffix=".png", dir=config.get_temp_dir(), delete=False)
    plt.savefig(figfile)
    filename = figfile.name
    figfile.close()

    return filename


def generate_date_avg_chart(url_stats, dates):

    fig = plt.figure(figsize=(7,4))

    fig.set_constrained_layout({"h_pad": 0.25, "w_pad": 3.0/72.0})

    ax = fig.add_subplot(111)

    values = []
    for datestr in dates:
        date_stat = url_stats["dates"].get(datestr)
        if date_stat is None:
            values.append(None)
        else:
            values.append(date_stat["avg_processing_time"])

    ax.plot(dates, values, label=url_stats["url"])

    plt.xticks(dates, rotation=270)
    plt.xlabel('Date')

    plt.ylabel('Seconds')
    plt.title('Average processing time per day')

    ax.grid(True)

    figfile = tempfile.NamedTemporaryFile(suffix=".png", dir=config.get_temp_dir(), delete=False)
    plt.savefig(figfile)
    filename = figfile.name
    figfile.close()

    return filename


def query_request_type_data(statsdb, from_date, to_date, exclude_dates):
    result_stats = {}
    request_types = []
    dates = []

    request_types_curs = statsdb.query_request_types()
    for row in request_types_curs:
        request_types.append(row["request_type"])
        result_stats[row["request_type"]] = {}

    day_delta = datetime.timedelta(days=1)
    enum_date = from_date
    while enum_date <= to_date:
        if enum_date not in exclude_dates:
            dates.append(enum_date)

            row_curs = statsdb.query_request_type(enum_date)
            for row in row_curs:
                result_stats[row["request_type"]][enum_date] = {"request_type": row["request_type"],
                                                                "date": enum_date,
                                                                "sum_request_count": row["sum_request_count"],
                                                                "sum_target_processing_time": row["sum_target_processing_time"],
                                                                "avg_target_processing_time": row["avg_target_processing_time"]}

        enum_date = (datetime.datetime.strptime(enum_date, '%Y-%m-%d') + day_delta).strftime('%Y-%m-%d')

    return request_types, dates, result_stats


def generate_request_type_table(styles, request_types, request_type_dates, request_type_stats):
    span_groups = []
    cur_span_group = None

    color_groups = []

    table_data = []

    header_row1 = []
    header_row2 = []
    header_row3 = []
    header_row1.append(Paragraph("", style=styles["table_header_center"]))
    header_row2.append(Paragraph("", style=styles["table_header_center"]))
    header_row3.append(Paragraph("Request Type", style=styles["table_header_center"]))

    prev_month_part = ""

    widths = [4 * cm]

    colnum = 1
    for datestr in request_type_dates:
        widths.append(2 * cm)
        month_part = datestr[:7]
        if month_part != prev_month_part:
            if cur_span_group is not None:
                cur_span_group["right"] = colnum - 1
            cur_span_group = {"left": colnum, "top": 0, "bottom": 0}
            span_groups.append(cur_span_group)
            prev_month_part = month_part
            header_row1.append(Paragraph(month_part, style=styles["table_header_left"]))
        else:
            header_row1.append(Paragraph("", style=styles["table_header_center"]))

        header_row2.append(Paragraph(datestr[8:11], style=styles["table_header_center"]))
        daycode = datetime.datetime.strptime(datestr, "%Y-%m-%d").strftime("%a")
        header_row3.append(Paragraph(daycode, style=styles["table_header_center"]))

        colnum += 1

    cur_span_group["right"] = colnum - 1

    table_data.append(header_row1)
    table_data.append(header_row2)
    table_data.append(header_row3)

    date_totals = {}

    for request_type in request_types:
        data_row = []
        data_row.append(Paragraph("{}".format(REQUEST_TYPE_MAP[request_type]), styles["table_data_left"]))
        for datestr in request_type_dates:
            date_stats = request_type_stats[request_type].get(datestr)
            if date_stats is None:
                data_row.append(Paragraph("{:,d}".format(0), styles["table_data_right"]))
            else:
                data_row.append(Paragraph("{:,d}".format(date_stats["sum_request_count"]), styles["table_data_right"]))
                date_total = date_totals.get(datestr)
                if date_total is None:
                    date_totals[datestr] = date_stats["sum_request_count"]
                else:
                    date_totals[datestr] += date_stats["sum_request_count"]

        table_data.append(data_row)

    data_row = []
    data_row.append(Paragraph("{}".format("TOTAL"), styles["table_data_right_bold"]))
    for datestr in request_type_dates:
        data_row.append(Paragraph("{:,d}".format(date_totals[datestr]), styles["table_data_right"]))
    table_data.append(data_row)

    t = Table(table_data, colWidths=widths)
    t.setStyle(define_request_type_table_style(span_groups, color_groups))

    return t


def generate_request_type_chart(request_types, request_type_dates, request_type_stats):
    fig = plt.figure(figsize=(15,7.5))

    fig.set_constrained_layout({"h_pad": 0.25, "w_pad": 3.0/72.0})

    ax = fig.add_subplot(111)

    for request_type in request_types:
        chart_data = []
        for datestr in request_type_dates:
            date_stats = request_type_stats[request_type].get(datestr)
            if date_stats is None:
                chart_data.append(None)
            else:
                chart_data.append(date_stats["sum_request_count"])

        ax.plot(request_type_dates, chart_data, label=REQUEST_TYPE_MAP[request_type])

    plt.xticks(request_type_dates, rotation=270)
    plt.xlabel('Date')

    plt.ylabel('Count')
    plt.title('Request count per type')

    handles, labels = ax.get_legend_handles_labels()

    lgd = ax.legend(handles, labels, loc=(1.05, 0.60), )

    ax.grid(True)

    figfile = tempfile.NamedTemporaryFile(suffix=".png", dir=config.get_temp_dir(), delete=False)
    plt.savefig(figfile)
    filename = figfile.name
    figfile.close()

    return filename

# ------------------------------ MAIN PROGRAM ------------------------------

args = read_arguments()

alblogs.initialize(args)

LOGGER = logging.getLogger(__name__)
LOGGER.info("Starting create_trend_report.py")

config = alblogs.get_configuration()

statsdb_file = "{}/stats.db".format(config.get_data_dir())
statsdb = alblogs.open_statsdb(statsdb_file, create=True)

if args.fromdate is None:
    from_date = statsdb.query_min_date()
    LOGGER.info("no fromdate specified, queried database for lowest date: {}".format(from_date))
else:
    from_date = args.fromdate
    LOGGER.info("fromdate specified on command line: {}".format(from_date))

if args.todate is None:
    to_date = statsdb.query_max_date()
    LOGGER.info("No todate specified, queried database for highest date: {}".format(to_date))
else:
    to_date = args.todate, '%Y-%m-%d'
    LOGGER.info("todate specified on command line: {}".format(to_date))

doc = SimpleDocTemplate("{}/trends-{}-{}.pdf".format(config.get_reports_dir(), from_date, to_date)
                        , pagesize=landscape(A4)
                        , leftMargin=1 * cm
                        , rightMargin=1 * cm
                        , topMargin=1 * cm
                        , bottomMargin=1 * cm)

styles = define_paragraph_styles()

# container for the 'Flowable' objects
elements = []

exclude_dates_dt = config.reports.trend.get_exclude_dates()
exclude_dates = []
for date_dt in exclude_dates_dt:
    exclude_dates.append(date_dt.strftime("%Y-%m-%d"))

# ----------------------------------

elements.append(Paragraph("Request Performance Trend Overview (excluding 5xx errors)", styles["table_title"]))
elements.append(Spacer(1, 0.2*cm))
elements.append(Paragraph("Based on top 10 requests by total processing time per day (excluding 5xx errors)", styles["table_subtitle"]))
elements.append(Spacer(1, 1*cm))

trend_data, dates = query_top_10_trend_data_excl_5xx(statsdb, from_date, to_date, exclude_dates)
trend_table = generate_trend_table(styles, trend_data, dates)

elements.append(trend_table)

elements.append(Spacer(1, 1*cm))

legend_table = generate_trend_legend_table(styles)
elements.append(legend_table)

elements.append(PageBreak())

# ----------------------------------

elements.append(Paragraph("Request Performance Trend Overview", styles["table_title"]))
elements.append(Spacer(1, 0.2*cm))
elements.append(Paragraph("Based on top 10 requests by total processing time per day", styles["table_subtitle"]))
elements.append(Spacer(1, 1*cm))

trend_data, dates = query_top_10_trend_data(statsdb, from_date, to_date, exclude_dates)
trend_table = generate_trend_table(styles, trend_data, dates)

elements.append(trend_table)

elements.append(Spacer(1, 1*cm))

legend_table = generate_trend_legend_table(styles)
elements.append(legend_table)

elements.append(PageBreak())

# ----------------------------------

tempfiles = []

request_volume_dates, request_volume_data = query_request_volume_data(statsdb, from_date, to_date, exclude_dates)
request_volume_table = generate_request_volume_table(styles, request_volume_data, request_volume_dates)

request_volume_chart_count_f = generate_request_volume_count_chart(request_volume_data, request_volume_dates)
request_volume_chart_count_h = open(request_volume_chart_count_f, "rb")
img_count = Image(request_volume_chart_count_h, width=(7*cm)*2, height=(4*cm)*2)
tempfiles.append({"name": request_volume_chart_count_f, "handle": request_volume_chart_count_h})

request_volume_chart_time_f = generate_request_volume_time_chart(request_volume_data, request_volume_dates)
request_volume_chart_time_h = open(request_volume_chart_time_f, "rb")
img_time = Image(request_volume_chart_time_h, width=(7*cm)*2, height=(4*cm)*2)
tempfiles.append({"name": request_volume_chart_time_f, "handle": request_volume_chart_time_h})

request_volume_chart_avg_f = generate_request_volume_avg_chart(request_volume_data, request_volume_dates)
request_volume_chart_avg_h = open(request_volume_chart_avg_f, "rb")
img_avg = Image(request_volume_chart_avg_h, width=(7*cm)*2, height=(4*cm)*2)
tempfiles.append({"name": request_volume_chart_avg_f, "handle": request_volume_chart_avg_h})

page_table = Table([[request_volume_table, img_count], [img_avg, img_time]])
page_table.setStyle(define_page_table_style_no_span())

elements.append(Paragraph("Total request volume", styles["table_title"]))
elements.append(Spacer(1, 1 * cm))
elements.append(page_table)

elements.append(PageBreak())

# ----------------------------------

elements.append(Paragraph("Request Types Overview", styles["table_title"]))
elements.append(Spacer(1, 1*cm))

request_types, request_type_dates, request_type_stats = query_request_type_data(statsdb, from_date, to_date, exclude_dates)
request_type_table = generate_request_type_table(styles, request_types, request_type_dates, request_type_stats)

elements.append(request_type_table)
elements.append(Spacer(1, 0.5*cm))

request_type_chart_filename = generate_request_type_chart(request_types, request_type_dates, request_type_stats)
request_type_chart_handle = open(request_type_chart_filename, 'rb')
img_request_type = Image(request_type_chart_handle, width=(20 * cm), height=(10 * cm))

elements.append(img_request_type)
tempfiles.append({"name": request_type_chart_filename, "handle": request_type_chart_handle})

# ----------------------------------

urls = sorted(trend_data.keys())
for url in urls:
    display_url = urllib.parse.urlparse(url).path
    url_stats = trend_data[url]

    elements.append(PageBreak())

    elements.append(Paragraph("Performance Trend for URL", styles["table_title"]))
    elements.append(Spacer(1, 0.2*cm))
    elements.append(Paragraph("{}".format(display_url), styles["table_subtitle"]))
    elements.append(Spacer(1, 1*cm))

    url_table = generate_url_table(styles, url_stats, dates)

    chart_count_filename = generate_date_count_chart(url_stats, dates)
    chart_count_handle = open(chart_count_filename, 'rb')
    img_count = Image(chart_count_handle, width=(7 * cm) * 2, height=(4 * cm) * 2)

    chart_avg_filename = generate_date_avg_chart(url_stats, dates)
    chart_avg_handle = open(chart_avg_filename, 'rb')
    img_avg = Image(chart_avg_handle, width=(7 * cm) * 2, height=(4 * cm) * 2)

    chart_time_filename = generate_date_time_chart(url_stats, dates)
    chart_time_handle = open(chart_time_filename, 'rb')
    img_time = Image(chart_time_handle, width=(7 * cm) * 2, height=(4 * cm) * 2)

    page_table = Table([[url_table, img_count], [img_avg, img_time]])
    page_table.setStyle(define_page_table_style_no_span())

    elements.append(page_table)
    tempfiles.append({"name": chart_count_filename, "handle": chart_count_handle})
    tempfiles.append({"name": chart_time_filename, "handle": chart_time_handle})
    tempfiles.append({"name": chart_avg_filename, "handle": chart_avg_handle})

LOGGER.info("Writing PDF")
doc.multiBuild(elements, canvasmaker=FooterCanvas)
LOGGER.info("Finished writing PDF")

LOGGER.info("Cleaning up temporary files")
for file_rec in tempfiles:
    LOGGER.debug("Removing temporary file '{}'".format(file_rec["name"]))
    file_rec["handle"].close()
    os.remove(file_rec["name"])

LOGGER.info("Finished")

