import alblogs
import logging
import datetime
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
        self.drawString(1 * cm, A4[0] - (0.90*cm), "Application Load Balancer Statistics")
        self.drawRightString(A4[1] - (1*cm), A4[0] - (0.90*cm), "Logs for date: {}".format(args.date))

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
    argparser.add_argument("date", metavar="DATE", type=str, help="Date for which to process ALB logs (yyyy-mm-dd)")

    args = argparser.parse_args()

    # try to convert args.date to check if it is a valid date
    datetime.datetime.strptime(args.date, '%Y-%m-%d')

    return args


def get_day_totals(statsdb, datestr):
    result = {"count": 0, "time": 0}

    rows = statsdb.query_day_totals(datestr)
    row = rows.fetchone()

    result["count"] = row["request_count"]
    result["time"] = row["target_processing_time_sec"]

    return result


def get_target_address_stats(statsdb, datestr):
    targets = {}
    hours = {}

    rows = statsdb.query_target_address_stats(datestr)
    for row in rows:
        target_rec = targets.get(row["target_address"])
        if target_rec is None:
            target_rec = {"total": 0, "hours": {}}
            targets[row["target_address"]] = target_rec

        target_rec["hours"][row["hour"]] = row["request_count"]
        target_rec["total"] += row["request_count"]

        hour_total = hours.get(row["hour"])
        if hour_total is None:
            hour_total = 0
        hours[row["hour"]] = hour_total + row["request_count"]

    return hours, targets


def process_target_stats(target_stats, hour_stats):
    target_addresses = sorted(target_stats.keys())

    chart_data = {"hours": [], "targets_pct": {}, "targets_val": {}}

    for target_address in target_addresses:
        chart_data["targets_val"][target_address] = []
        chart_data["targets_pct"][target_address] = []

    for hour in range(24):
        chart_data["hours"].append(hour)
        for target_address in target_addresses:
            value = target_stats[target_address]["hours"].get(hour)
            if value is None:
                value = 0

            total = hour_stats.get(hour)
            if total is None:
                pct = 0
            else:
                pct = (value / total) * 100.0

            chart_data["targets_pct"][target_address].append(pct)
            chart_data["targets_val"][target_address].append(value)

    return target_addresses, chart_data


def generate_target_chart(target_addresses, chart_data):

    fig = plt.figure(figsize=(8,5))

    fig.set_constrained_layout({"h_pad": 0.25, "w_pad": 3.0/72.0})

    ax = fig.add_subplot(111)

    for target_address in target_addresses:
        ax.plot(chart_data["hours"], chart_data["targets_pct"][target_address], label=target_address)

    plt.xticks(chart_data["hours"])
    plt.xlabel('Time')

    yrange = numpy.arange(110, step=10)
    plt.yticks(yrange)
    plt.ylabel('Percentage')
    plt.title('Request percentage per target')

    handles, labels = ax.get_legend_handles_labels()

    lgd = ax.legend(handles, labels, loc=(0.80, 1.05), )

    ax.grid(True)
    ax.set_ylim(ymin=0, ymax=100)
    ax.set_xlim(xmin=0, xmax=23)

    figfile = tempfile.NamedTemporaryFile(suffix=".png", dir=config.get_temp_dir(), delete=False)
    plt.savefig(figfile)
    filename = figfile.name
    figfile.close()

    return filename


def generate_table_rows(target_addresses, chart_data):
    rows = []

    for hr in range(24):
        row = []
        row.append(hr)

        for target_address in target_addresses:
            row.append(chart_data["targets_val"][target_address][hr])
            row.append(chart_data["targets_pct"][target_address][hr])
        rows.append(row)

    return rows


def define_page_table_style():
    table_style = TableStyle()
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")
    table_style.add('ALIGN', (0, 0), (-1, -1), "CENTER")

    return table_style


def define_target_table_style():
    table_style = TableStyle()

    # All rows
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")

    table_style.add('INNERGRID', (0, 1), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 1), (-1, -1), 0.4 * mm, colors.black)
    table_style.add('BOX', (0, 1), (-1, -2), 0.4 * mm, colors.black)

    # Header row
    table_style.add('INNERGRID', (0, 0), (-1, 0), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, 0), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (-1, 1), colors.black)
    table_style.add('ALIGN', (0, 0), (-1, 0), "CENTER")

    table_style.add('SPAN', (1, 0), (2, 0))
    table_style.add('SPAN', (3, 0), (4, 0))

    return table_style


def define_totals_table_style():
    table_style = TableStyle()

    # All rows
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")

    table_style.add('INNERGRID', (0, 1), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 1), (-1, -1), 0.4 * mm, colors.black)
    table_style.add('BOX', (0, 1), (-1, -2), 0.4 * mm, colors.black)

    # Header column
    table_style.add('INNERGRID', (0, 0), (-1, 0), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, 0), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (1, 0), colors.black)
    #table_style.add('BACKGROUND', (0, 0), (0, -1), colors.black)

    table_style.add('SPAN', (0, 0), (1, 0))

    return table_style


def define_url_table_style():
    table_style = TableStyle()

    # All rows
    table_style.add('TOPPADDING', (0, 0), (-1, -1), 0)
    table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 0)
    table_style.add('VALIGN', (0, 0), (-1, -1), "MIDDLE")

    table_style.add('INNERGRID', (0, 1), (-1, -1), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 1), (-1, -1), 0.4 * mm, colors.black)
    table_style.add('BOX', (0, 1), (-1, -2), 0.4 * mm, colors.black)

    # Header row
    table_style.add('INNERGRID', (0, 0), (-1, 0), 0.2 * mm, colors.black)
    table_style.add('BOX', (0, 0), (-1, 0), 0.4 * mm, colors.black)

    table_style.add('BACKGROUND', (0, 0), (-1, 0), colors.black)
    table_style.add('ALIGN', (0, 0), (-1, 0), "CENTER")

    return table_style


def define_paragraph_styles():
    styles = {}

    styles["table_title"] = ParagraphStyle('table_title', fontName=DEFAULT_FONT, fontSize=16, textColor=colors.black, alignment=TA_CENTER)
    styles["table_header_left"] = ParagraphStyle('table_header_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.white, alignment=TA_LEFT)
    styles["table_header_center"] = ParagraphStyle('table_header_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.white, alignment=TA_CENTER)
    styles["table_header_right"] = ParagraphStyle('table_header_right', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.white, alignment=TA_RIGHT)
    styles["table_data_left"] = ParagraphStyle('table_data_left', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.white, alignment=TA_LEFT)
    styles["table_data_right"] = ParagraphStyle('table_data_right', fontName=DEFAULT_FONT, fontSize=8, textColor=colors.black, backColor=colors.white, alignment=TA_RIGHT)
    styles["table_data_right_bold"] = ParagraphStyle('table_data_right', fontName=DEFAULT_FONT_BOLD, fontSize=8, textColor=colors.black, backColor=colors.white, alignment=TA_RIGHT)

    return styles


def generate_target_table(styles, target_addresses, chart_data):
    table_data = []

    header_row1 = []
    header_row2 = []

    header_row1.append("")
    header_row2.append(Paragraph("Hour", styles["table_header_center"]))

    totals = {}

    for target_address in target_addresses:
        header_row1.append(Paragraph("{}".format(target_address), styles["table_header_center"]))
        header_row1.append("")

        header_row2.append(Paragraph("Count", styles["table_header_center"]))
        header_row2.append(Paragraph("%", styles["table_header_center"]))
        totals[target_address] = 0

    table_data.append(header_row1)
    table_data.append(header_row2)

    total_count = 0
    for hour in chart_data["hours"]:
        table_row = []
        table_row.append(Paragraph("{}".format(hour), styles["table_data_right"]))

        for target_address in target_addresses:
            totals[target_address] += chart_data["targets_val"][target_address][hour]
            total_count += chart_data["targets_val"][target_address][hour]
            table_row.append(
                Paragraph("{:,d}".format(chart_data["targets_val"][target_address][hour]), styles["table_data_right"]))
            table_row.append(Paragraph("{:0,.2f}".format(chart_data["targets_pct"][target_address][hour]),
                                       styles["table_data_right"]))
        table_data.append(table_row)

    last_row = []
    last_row.append(Paragraph("TOT", styles["table_data_right_bold"]))

    for target_address in target_addresses:
        last_row.append(Paragraph("{:,d}".format(totals[target_address]),  styles["table_data_right_bold"]))
        pct = (float(totals[target_address]) / float(total_count)) * 100.0
        last_row.append(Paragraph("{:0,.2f}".format(pct),  styles["table_data_right_bold"]))

    table_data.append(last_row)

    t = Table(table_data, colWidths=[1.1 * cm, 1.7 * cm, 1.7 * cm, 1.7 * cm, 1.7 * cm])
    t.setStyle(define_target_table_style())

    return t


def get_top_10_by_count_stats(statsdb, datestr):
    url_stats_curs = statsdb.query_top_10_url_by_count(datestr)
    url_stats = []

    total_request_count = 0
    for row in url_stats_curs:
        stats = {"url": row["url"], "request_count": row["sum_request_count"],
                 "processing_time": row["sum_target_processing_time"]}
        url_stats.append(stats)
        total_request_count += row["sum_request_count"]

    # second iteration, adding calculated fields
    for stats in url_stats:
        stats["top_10_pct"] = (stats["request_count"] / total_request_count) * 100
        stats["total_pct"] = (stats["request_count"] / day_totals["count"]) * 100
        stats["avg_processing_time"] = stats["processing_time"] / stats["request_count"]

    return url_stats


def get_top_10_by_time_stats(statsdb, datestr):
    url_stats_curs = statsdb.query_top_10_url_by_time(datestr)
    url_stats = []

    total_request_time = 0
    for row in url_stats_curs:
        stats = {"url": row["url"], "request_count": row["sum_request_count"],
                 "processing_time": row["sum_target_processing_time"]}
        url_stats.append(stats)
        total_request_time += row["sum_target_processing_time"]

    # second iteration, adding calculated fields
    for stats in url_stats:
        stats["top_10_pct"] = (stats["processing_time"] / total_request_time) * 100
        stats["total_pct"] = (stats["processing_time"] / day_totals["time"]) * 100
        stats["avg_processing_time"] = stats["processing_time"] / stats["request_count"]

    return url_stats


def get_top_100_average_processing_time_stats(statsdb, datestr):
    url_stats_curs = statsdb.query_top_100_average_processing_time(datestr)
    url_stats = []

    for row in url_stats_curs:
        stats = {"url": row["url"],
                 "sum_request_count": row["sum_request_count"],
                 "avg_target_processing_time": row["avg_target_processing_time"],
                 "sum_target_processing_time": row["sum_target_processing_time"],
                 }
        url_stats.append(stats)

    return url_stats


def generate_url_count_table(styles, url_stats):
    table_data = []
    header_row = []
    header_row.append(Paragraph("Url", styles["table_header_center"]))
    header_row.append(Paragraph("Requests", styles["table_header_center"]))
    header_row.append(Paragraph("% of top 10", styles["table_header_center"]))
    header_row.append(Paragraph("% of total", styles["table_header_center"]))
    header_row.append(Paragraph("Tot Time", styles["table_header_center"]))
    header_row.append(Paragraph("Avg Time", styles["table_header_center"]))

    table_data.append(header_row)

    totals = {"request_count": 0, "top_10_pct": 0, "total_pct": 0, "processing_time": 0}
    for stats in url_stats:
        totals["request_count"] += stats["request_count"]
        totals["top_10_pct"] += stats["top_10_pct"]
        totals["total_pct"] += stats["total_pct"]
        totals["processing_time"] += stats["processing_time"]

        data_row = []
        data_row.append(Paragraph(stats["url"], styles["table_data_left"]))
        data_row.append(Paragraph("{0:,}".format(stats["request_count"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,.2f}".format(stats["top_10_pct"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,.2f}".format(stats["total_pct"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,.0f}".format(stats["processing_time"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,.5f}".format(stats["avg_processing_time"]), styles["table_data_right"]))

        table_data.append(data_row)

    last_row = []
    last_row.append(Paragraph("TOTAL", styles["table_data_right_bold"]))
    last_row.append(Paragraph("{0:,}".format(totals["request_count"]), styles["table_data_right_bold"]))
    last_row.append(Paragraph("{:0,.2f}".format(totals["top_10_pct"]), styles["table_data_right_bold"]))
    last_row.append(Paragraph("{:0,.2f}".format(totals["total_pct"]), styles["table_data_right_bold"]))
    last_row.append(Paragraph("{:0,.0f}".format(totals["processing_time"]), styles["table_data_right_bold"]))
    last_row.append(Paragraph("", styles["table_data_right_bold"]))

    table_data.append(last_row)

    t = Table(table_data)
    t.setStyle(define_url_table_style())

    return t


def generate_url_time_table(styles, url_stats):
    table_data = []
    header_row = []
    header_row.append(Paragraph("Url", styles["table_header_center"]))
    header_row.append(Paragraph("Tot Time", styles["table_header_center"]))
    header_row.append(Paragraph("% of top 10", styles["table_header_center"]))
    header_row.append(Paragraph("% of total", styles["table_header_center"]))
    header_row.append(Paragraph("Requests", styles["table_header_center"]))
    header_row.append(Paragraph("Avg Time", styles["table_header_center"]))

    table_data.append(header_row)

    totals = {"request_count": 0, "top_10_pct": 0, "total_pct": 0, "processing_time": 0}
    for stats in url_stats:
        totals["request_count"] += stats["request_count"]
        totals["top_10_pct"] += stats["top_10_pct"]
        totals["total_pct"] += stats["total_pct"]
        totals["processing_time"] += stats["processing_time"]

        data_row = []
        data_row.append(Paragraph(stats["url"], styles["table_data_left"]))
        data_row.append(Paragraph("{:0,.0f}".format(stats["processing_time"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,.2f}".format(stats["top_10_pct"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,.2f}".format(stats["total_pct"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,d}".format(stats["request_count"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,.5f}".format(stats["avg_processing_time"]), styles["table_data_right"]))

        table_data.append(data_row)

    last_row = []
    last_row.append(Paragraph("TOTAL", styles["table_data_right_bold"]))
    last_row.append(Paragraph("{0:,}".format(totals["request_count"]), styles["table_data_right_bold"]))
    last_row.append(Paragraph("{:0,.2f}".format(totals["top_10_pct"]), styles["table_data_right_bold"]))
    last_row.append(Paragraph("{:0,.2f}".format(totals["total_pct"]), styles["table_data_right_bold"]))
    last_row.append(Paragraph("{:0,.0f}".format(totals["processing_time"]), styles["table_data_right_bold"]))
    last_row.append(Paragraph("", styles["table_data_right_bold"]))

    table_data.append(last_row)

    t = Table(table_data)
    t.setStyle(define_url_table_style())

    return t


def generate_url_top100_table(styles, url_stats):
    table_data = []
    header_row = []
    header_row.append(Paragraph("Pos", styles["table_header_center"]))
    header_row.append(Paragraph("Url", styles["table_header_center"]))
    header_row.append(Paragraph("Avg Time", styles["table_header_center"]))
    header_row.append(Paragraph("Tot Time", styles["table_header_center"]))
    header_row.append(Paragraph("Requests", styles["table_header_center"]))

    table_data.append(header_row)

    pos = 1
    for stats in url_stats:
        data_row = []
        data_row.append(Paragraph("{:0,d}".format(pos), styles["table_data_right"]))
        data_row.append(Paragraph(stats["url"], styles["table_data_left"]))
        data_row.append(Paragraph("{:0,.5f}".format(stats["avg_target_processing_time"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,.0f}".format(stats["sum_target_processing_time"]), styles["table_data_right"]))
        data_row.append(Paragraph("{:0,d}".format(stats["sum_request_count"]), styles["table_data_right"]))
        pos += 1
        table_data.append(data_row)

    colWidths = [1 * cm, A4[1] - (7*cm) - (2*cm), 2 * cm, 2 * cm, 2 * cm]
    t = Table(table_data, colWidths=colWidths, repeatRows=1)
    t.setStyle(define_url_table_style())
    return t


def generate_totals_table(styles, totals):
    table_rows = []

    table_row = []
    table_row.append(Paragraph("Total requests", styles["table_header_center"]))
    table_row.append(Paragraph("", style=styles["table_data_right"]))
    table_rows.append(table_row)

    table_row = []
    table_row.append(Paragraph("Request count", styles["table_data_left"]))
    table_row.append(Paragraph("{:,d}".format(day_totals["count"]), style=styles["table_data_right"]))
    table_rows.append(table_row)

    table_row = []
    table_row.append(Paragraph("Procesing time (sec)", styles["table_data_left"]))
    table_row.append(Paragraph("{:0,.0f}".format(day_totals["time"]), style=styles["table_data_right"]))
    table_rows.append(table_row)

    t = Table(table_rows, colWidths=[3 * cm, 2 * cm])
    t.setStyle(define_totals_table_style())

    return t

# ------------------------------ MAIN PROGRAM ------------------------------


args = read_arguments()

alblogs.initialize(args)

LOGGER = logging.getLogger(__name__)
LOGGER.info("Starting update_stats.py")

config = alblogs.get_configuration()

logsdb_file = "{}/{}-alblogs.db".format(config.get_data_dir(), args.date)
statsdb_file = "{}/stats.db".format(config.get_data_dir(), args.date)

statsdb = alblogs.open_statsdb(statsdb_file, create=True)

day_totals = get_day_totals(statsdb, args.date)
hour_stats, target_stats = get_target_address_stats(statsdb, args.date)

target_addresses, chart_data = process_target_stats(target_stats, hour_stats)

target_chart_filename = generate_target_chart(target_addresses, chart_data)

doc = SimpleDocTemplate("{}/{}-report.pdf".format(config.get_reports_dir(), args.date)
                        , pagesize=landscape(A4)
                        , leftMargin=1 * cm
                        , rightMargin=1 * cm
                        , topMargin=1 * cm
                        , bottomMargin=1 * cm)


styles = define_paragraph_styles()

# container for the 'Flowable' objects
elements = []

elements.append(Paragraph("Load Balancer Statistics", styles["table_title"]))
elements.append(Spacer(1, 2*cm))

target_table = generate_target_table(styles, target_addresses, chart_data)

chart_handle = open(target_chart_filename, 'rb')

img = Image(chart_handle, width=(8*cm)*2, height=(5*cm)*2)

page_table = Table([[target_table, img]])
page_table.setStyle(define_page_table_style())

elements.append(page_table)

elements.append(PageBreak())
elements.append(Paragraph("Top 10 requests by total request count", styles["table_title"]))
elements.append(Spacer(1, 0.5*cm))

totals_table = generate_totals_table(styles, day_totals)
elements.append(totals_table)

elements.append(Spacer(1, 0.5*cm))

url_count_stats = get_top_10_by_count_stats(statsdb, args.date)
url_count_table = generate_url_count_table(styles, url_count_stats)

elements.append(url_count_table)

elements.append(PageBreak())
elements.append(Paragraph("Top 10 requests by total processing time", styles["table_title"]))
elements.append(Spacer(1, 0.5*cm))

totals_table = generate_totals_table(styles, day_totals)
elements.append(totals_table)

elements.append(Spacer(1, 0.5*cm))

url_time_stats = get_top_10_by_time_stats(statsdb, args.date)
url_time_table = generate_url_time_table(styles, url_time_stats)
elements.append(url_time_table)

elements.append(PageBreak())
elements.append(Paragraph("Top 100 requests by average processing time", styles["table_title"]))
elements.append(Spacer(1, 0.5*cm))

url_top100_stats = get_top_100_average_processing_time_stats(statsdb, args.date)
url_top100_table = generate_url_top100_table(styles, url_top100_stats)
elements.append(url_top100_table)

# write the document to disk
doc.multiBuild(elements, canvasmaker=FooterCanvas)

chart_handle.close()

# delete temporary chart file
os.remove(target_chart_filename)

print(A4)