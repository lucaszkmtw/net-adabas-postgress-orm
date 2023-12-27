import logging
import csv
import io

from datetime import datetime


class CsvFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL)

    def format(self, record):
        levelname = str(record.levelname)
        if ':' in levelname:
            levelname = levelname.split(':')[-1]
        self.writer.writerow([
            record.levelname,
            record.msg,
            datetime.fromtimestamp(record.created)])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()


logging.basicConfig(filename="logs/log.csv", level=logging.DEBUG)

logger = logging.getLogger(__name__)
logging.root.handlers[0].setFormatter(CsvFormatter())
