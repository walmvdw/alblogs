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


def query_top_10_trend_data(statsdb, from_date, to_date):
    result = {}
    dates = []
    day_delta = datetime.timedelta(days=1)
    enum_date = from_date
    while enum_date <= to_date:
        datestr = enum_date
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

    table_style.add('INNERGRID', (0, 1), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 2), (-1, -1), 0.4 * mm, colors.black)

    # Header row
    table_style.add('INNERGRID', (0, 0), (-1, 0), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, 1), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (-1, 1), colors.black)
    table_style.add('ALIGN', (0, 0), (-1, 0), "CENTER")

    for span_group in span_groups:
        table_style.add('SPAN', (span_group["first"], 0), (span_group["last"], 0))

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
    header_row1.append(Paragraph("", style=styles["table_header_center"]))
    header_row2.append(Paragraph("URL Path", style=styles["table_header_center"]))

    prev_month_part = ""

    widths = [13 * cm]

    colnum = 1
    for datestr in dates:
        widths.append(1.4 * cm)
        month_part = datestr[:7]
        if month_part != prev_month_part:
            if cur_span_group is not None:
                cur_span_group["last"] = colnum - 1
            cur_span_group = {"first": colnum}
            span_groups.append(cur_span_group)
            prev_month_part = month_part
            header_row1.append(Paragraph(month_part, style=styles["table_header_left"]))
        else:
            header_row1.append(Paragraph("", style=styles["table_header_center"]))

        header_row2.append(Paragraph(datestr[8:11], style=styles["table_header_center"]))

        colnum += 1

    cur_span_group["last"] = colnum - 1

    header_row1.append("")
    header_row2.append("")
    widths.append(0.2 * cm)
    colnum += 1

    color_groups.append({"row": 0, "col": colnum - 1, "color": colors.white})
    color_groups.append({"row": 1, "col": colnum - 1, "color": colors.white})

    cur_span_group = {"first": colnum}
    span_groups.append(cur_span_group)

    header_row1.append(Paragraph("Improvement", style=styles["table_header_center"]))
    header_row2.append(Paragraph("last/first", style=styles["table_header_left"]))
    widths.append(1.5 * cm)
    colnum += 1

    header_row1.append(Paragraph("", style=styles["table_header_center"]))
    header_row2.append(Paragraph("max/min", style=styles["table_header_center"]))
    widths.append(1.5 * cm)
    colnum += 1

    cur_span_group["last"] = colnum - 1

    table_data.append(header_row1)
    table_data.append(header_row2)

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


def generate_date_chart(url_stats, dates):

    fig = plt.figure(figsize=(8,5))

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
    plt.title('Average processing time')

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

    return table_style


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

elements.append(Paragraph("Request Performance Trend Overview", styles["table_title"]))
elements.append(Spacer(1, 0.2*cm))
elements.append(Paragraph("Based on top 10 requests by total processing time per day", styles["table_subtitle"]))
elements.append(Spacer(1, 1*cm))

trend_data, dates = query_top_10_trend_data(statsdb, from_date, to_date)
trend_table = generate_trend_table(styles, trend_data, dates)

elements.append(trend_table)

elements.append(Spacer(1, 1*cm))

legend_table = generate_trend_legend_table(styles)
elements.append(legend_table)

tempfiles = []

urls = sorted(trend_data.keys())
for url in urls:
    display_url = urllib.parse.urlparse(url).path
    url_stats = trend_data[url]

    elements.append(PageBreak())

    elements.append(Paragraph("Performance Trend for URL", styles["table_subtitle"]))
    elements.append(Spacer(1, 0.2*cm))
    elements.append(Paragraph("{}".format(display_url), styles["table_title"]))
    elements.append(Spacer(1, 1*cm))

    url_table = generate_url_table(styles, url_stats, dates)

    chart_filename = generate_date_chart(url_stats, dates)

    chart_handle = open(chart_filename, 'rb')

    img = Image(chart_handle, width=(8 * cm) * 2, height=(5 * cm) * 2)

    page_table = Table([[url_table, img]])
    page_table.setStyle(define_page_table_style())

    elements.append(page_table)
    tempfiles.append({"name": chart_filename, "handle": chart_handle})

doc.multiBuild(elements, canvasmaker=FooterCanvas)

for file_rec in tempfiles:
    file_rec["handle"].close()
    os.remove(file_rec["name"])
