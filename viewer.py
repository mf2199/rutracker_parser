#!/usr/bin/env python3

import io
import sys
import urllib.parse
from datetime import datetime
from tarfile import TarFile

from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import *

TREE_COLUMNS = (
    "id",
    "name",
    "size",
    "seeds",
    "peers",
    "hash",
    "downloads",
    "date",
    "category",
)
TREE_COLUMNS_VISIBLE = (
    "ID",
    "Название",
    "Размер",
    "Сиды",
    "Пиры",
    "Hash",
    "Скачиваний",
    "Дата",
    "Раздел",
)


class NumberSortModel(QSortFilterProxyModel):
    @staticmethod
    def _scaled(value):
        if value[-2:] == " B":
            return float(value[:-2])
        elif value[-3:] == " KB":
            return float(value[:-3]) * 1024
        elif value[-3:] == " MB":
            return float(value[:-3]) * 1024 * 1024
        elif value[-3:] == " GB":
            return float(value[:-3]) * 1024 * 1024 * 1024

    def lessThan(self, left_, right_):
        if not left_.data():
            return True
        if not right_.data():
            return False

        if left_.column() in [
            TREE_COLUMNS.index("id"),
            TREE_COLUMNS.index("seeds"),
            TREE_COLUMNS.index("peers"),
            TREE_COLUMNS.index("downloads"),
        ]:
            lvalue = int(left_.data())
            rvalue = int(right_.data())
        elif left_.column() == TREE_COLUMNS.index("date"):
            try:
                lvalue = datetime.strptime(left_.data(), "%d-%b-%y %H:%M")
                rvalue = datetime.strptime(right_.data(), "%d-%b-%y %H:%M")
            except ValueError:
                lvalue = datetime.strptime(left_.data(), "%d-%m-%y %H:%M")
                rvalue = datetime.strptime(right_.data(), "%d-%m-%y %H:%M")
        elif left_.column() == TREE_COLUMNS.index("size"):
            lvalue = self._scaled(left_.data())
            rvalue = self._scaled(right_.data())
        else:
            lvalue = left_.data()
            rvalue = right_.data()
        return lvalue < rvalue


class MainWindow(QMainWindow):
    # noinspection PyUnresolvedReferences
    def __init__(self):
        super(MainWindow, self).__init__()
        frame = QFrame(self)

        self.result_count = 0
        self.found_items = []

        self.grid = QGridLayout(frame)
        self.setCentralWidget(frame)

        self.input = QLineEdit()
        self.input2 = QLineEdit()
        self.search = QPushButton()
        self.tree = QTableView()
        self.webview = QWebEngineView()
        separator = QSplitter()
        self.statusbar = QStatusBar()
        self.setStatusBar(self.statusbar)
        self.timer = QTimer(self)
        self.timer.setInterval(1000)
        self.timer.timeout.connect(self.do_update_table)

        self.model = QStandardItemModel()
        proxy = NumberSortModel()
        proxy.setSourceModel(self.model)
        self.tree.setModel(proxy)
        self.model.setColumnCount(len(TREE_COLUMNS))
        self.model.setHorizontalHeaderLabels(TREE_COLUMNS_VISIBLE)
        self.tree.verticalHeader().setVisible(False)
        self.tree.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tree.setSortingEnabled(True)
        self.tree.verticalHeader().setDefaultSectionSize(24)
        self.webview.setUrl(QUrl("about:blank"))
        self.webview.setZoomFactor(0.85)
        self.search.setText("Искать")
        self.input2.setMaximumWidth(300)
        self.setWindowTitle("RuTracker database   |   by strayge")
        self.input.setPlaceholderText("Строка для поиска в названии")
        self.input2.setPlaceholderText("Строка для поиска в категории")

        self.grid.addWidget(self.input, 0, 0)
        self.grid.addWidget(self.input2, 0, 1)

        self.grid.addWidget(self.search, 0, 2)
        self.grid.addWidget(separator, 1, 0, 2, 0)
        separator.addWidget(self.tree)
        separator.addWidget(self.webview)

        self.resize(1500, 800)
        self.tree.resize(2800, 0)

        self.search.clicked.connect(self.do_search)
        self.input.returnPressed.connect(self.do_search)
        self.input2.returnPressed.connect(self.do_search)
        self.tree.clicked.connect(self.do_select)
        self.tree.doubleClicked.connect(self.do_work)

        self.searcher = None
        self.first_result = False

    def do_update_table(self, finish=False):
        if finish:
            self.timer.stop()
        self.model.setRowCount(len(self.found_items))
        for i in range(self.result_count, len(self.found_items)):
            for j in range(len(TREE_COLUMNS)):
                item = self.found_items[i]
                qitem = QStandardItem()
                qitem.setData(QVariant(item[j]), Qt.ItemDataRole.DisplayRole)
                self.model.setItem(i, j, qitem)
            self.result_count += 1

        # self.tree.sortByColumn(
        #     tree_columns.index('seeds'), Qt.DescendingOrder
        # )

        self.tree.resizeColumnsToContents()
        if self.tree.columnWidth(TREE_COLUMNS.index("name")) > 500:
            self.tree.setColumnWidth(TREE_COLUMNS.index("name"), 500)
        if finish:
            self.timer.stop()
            self.statusbar.showMessage(
                f"Поиск закончен. Найдено {len(self.found_items)} записей"
            )
            self.search.setText("Поиск")
        else:
            self.statusbar.showMessage(
                f"Идет поиск... Найдено {self.result_count} записей"
            )

    def _add_found_item(self, item):
        for j in range(len(TREE_COLUMNS)):
            if j == TREE_COLUMNS.index("size"):
                size = int(item[j])
                if size < 1024:
                    item[j] = "%.0f B" % (float(item[j]))
                elif size < 1024 * 1024:
                    item[j] = "%.0f KB" % (float(item[j]) / 1024)
                elif size < 1024 * 1024 * 1024:
                    item[j] = "%.0f MB" % (float(item[j]) / (1024 * 1024))
                else:
                    item[j] = "%.2f GB" % (
                        float(item[j]) / (1024 * 1024 * 1024)
                    )
        self.found_items.append(item)

    def do_show_status(self, text):
        if text == "Поиск закончен.":
            self.do_update_table(True)
        else:
            self.statusbar.showMessage(
                text + " Найдено %i записей." % len(self.found_items)
            )

    def do_search(self):
        if self.search.text() == "Отмена":
            if self.searcher and self.searcher.isRunning():
                self.search.setText("Поиск")
                self.searcher.stop()
                self.timer.stop()
                return

        self.first_result = True
        self.search.setText("Отмена")
        self.result_count = 0
        self.found_items = []
        self.model.setRowCount(0)
        self.searcher = SearchThread(self.input.text(), self.input2.text())
        self.searcher.add_found_item.connect(self._add_found_item)
        self.searcher.status.connect(self.do_show_status)
        self.searcher.start(QThread.LowestPriority)
        self.timer.start()

    def do_work(self, index=None):
        index = self.tree.model().mapToSource(index)

        name = self.model.item(index.row(), TREE_COLUMNS.index("name")).text()
        hash_ = self.model.item(index.row(), TREE_COLUMNS.index("hash")).text()
        args = (
            ("magnet:?xt=urn:btih:", name),
            ("dn=", hash_),
            ("tr=", "udp://tracker.publicbt.com:80"),
            ("tr=", "udp://tracker.openbittorrent.com:80"),
            ("tr=", "tracker.ccc.de:80"),
            ("tr=", "tracker.istole.it:80"),
            ("tr=", "udp://tracker.publicbt.com:80"),
        )
        link = ""
        for i, j in args:
            link += i + urllib.parse.quote_plus(j).replace("+", "%20")
        # noinspection PyArgumentList
        QApplication.clipboard().setText(link)
        print("magnet link copied to clipboard.")

    def do_select(self, index=None):
        id_ = int(self.model.item(index.row(), TREE_COLUMNS.index("id")).text())
        try:
            archive = TarFile.open(
                f"desc/{id_ // 100000:03}/{id_ // 1000:05}.tar.bz2", "r:bz2"
            )
            s = archive.extractfile("%08i" % id_).read().decode()
            archive.close()
            self.webview.setHtml(s)
        except FileNotFoundError:
            self.webview.setHtml("Нет описания")


class SearchThread(QThread):
    add_found_item = pyqtSignal(object)
    status = pyqtSignal(object)

    def __init__(self, text, category):
        QThread.__init__(self)
        self.text = text
        self.category = category

    def stop(self):
        self.status.emit("Поиск остановлен.")
        self.terminate()

    def run(self):
        limit = 20
        text = self.text
        category = self.category

        words_contains = []
        words_not_contains = []
        words_category = []

        for w in text.split(" "):
            if (len(w) > 1) and (w[0]) == "-":
                words_not_contains.append(w[1:])
            elif (len(w) > len("limit:")) and (w[:6] == "limit:"):
                limit = int(w[6:])
            else:
                words_contains.append(w)
        for w in category.split(" "):
            words_category.append(w)

        archive = TarFile.open("table_sorted.tar.bz2", "r:bz2")
        member = archive.members[0]
        buffered_reader = archive.extractfile(member)
        buffered_text_reader = io.TextIOWrapper(
            buffered_reader, encoding="utf8"
        )

        count = 0

        for line in buffered_text_reader:
            item = line.strip().split(sep="\t")

            print(item)
            for i in range(len(TREE_COLUMNS)):
                print(f"{TREE_COLUMNS[i]}: {item[i]}")

            next_ = False

            for w in words_contains:
                if w.lower() in item[TREE_COLUMNS.index("name")].lower():
                    pass
                else:
                    next_ = True
                    break
            if next_:
                continue

            for w in words_not_contains:
                if w.lower() in item[TREE_COLUMNS.index("name")].lower():
                    next_ = True
                    break
            if next_:
                continue

            for w in words_category:
                if w.lower() in item[TREE_COLUMNS.index("category")].lower():
                    pass
                else:
                    next_ = True
                    break
            if next_:
                continue

            count += 1
            self.add_found_item.emit(item)

            if count >= limit:
                self.status.emit("Поиск закончен.")
                break


if __name__ == "__main__":
    app = QApplication(sys.argv)
    mainWin = MainWindow()
    mainWin.show()
    sys.exit(app.exec_())
