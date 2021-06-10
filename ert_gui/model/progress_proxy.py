import typing
from collections import defaultdict

from ert_gui.model.snapshot import ProgressRole
from qtpy.QtCore import QAbstractItemModel, QModelIndex, QSize, Qt, QVariant
from qtpy.QtGui import QColor, QFont


class ProgressProxyModel(QAbstractItemModel):
    def __init__(self, source_model: QAbstractItemModel, parent=None) -> None:
        QAbstractItemModel.__init__(self, parent)
        self._source_model = source_model
        self._progress = None
        self._connect()

    def _connect(self):
        self._source_model.dataChanged.connect(self._source_data_changed)
        self._source_model.rowsInserted.connect(self._source_rows_inserted)
        self._source_model.modelAboutToBeReset.connect(self.modelAboutToBeReset)
        self._source_model.modelReset.connect(self._source_reset)

        # rowCount-1 of the top index in the underlying, will be the last/most
        # recent iteration. If it's -1, then there are no iterations yet.
        last_iter = self._source_model.rowCount(QModelIndex()) - 1
        if last_iter >= 0:
            self._recalculate_progress(last_iter)

    def columnCount(self, parent=QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return 1

    def rowCount(self, parent=QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return 1

    def index(self, row: int, column: int, parent=QModelIndex()) -> QModelIndex:
        if parent.isValid():
            return QModelIndex()
        return self.createIndex(row, column, None)

    def parent(self, index: QModelIndex):
        return QModelIndex()

    def hasChildren(self, parent: QModelIndex) -> bool:
        return not parent.isValid()

    def data(self, index: QModelIndex, role=Qt.DisplayRole) -> QVariant:
        if not index.isValid():
            return QVariant()

        if role == Qt.TextAlignmentRole:
            return Qt.AlignCenter

        if role == ProgressRole:
            return self._progress

        if role in (Qt.StatusTipRole, Qt.WhatsThisRole, Qt.ToolTipRole):
            return ""

        if role == Qt.SizeHintRole:
            return QSize(30, 30)

        if role == Qt.FontRole:
            return QFont()

        if role in (Qt.BackgroundRole, Qt.ForegroundRole, Qt.DecorationRole):
            return QColor()

        if role == Qt.DisplayRole:
            return ""

        return QVariant()

    def _recalculate_progress(self, iter_):
        d = defaultdict(lambda: 0)
        nr_reals = 0
        current_iter_node = self._source_model.index(
            iter_, 0, QModelIndex()
        ).internalPointer()
        if current_iter_node is None:
            self._progress = None
            return
        for v in current_iter_node.children.values():
            ## realizations
            status = v.data["status"]
            nr_reals += 1
            d[status] += 1
        self._progress = {"status": d, "nr_reals": nr_reals}

    def _source_data_changed(
        self,
        top_left: QModelIndex,
        bottom_right: QModelIndex,
        roles: typing.List[int],
    ):
        p = top_left
        while p.parent().isValid():
            p = p.parent()
        self._recalculate_progress(p.row())
        index = self.index(0, 0, QModelIndex())
        self.dataChanged.emit(index, index, [ProgressRole])

    def _source_rows_inserted(self, parent: QModelIndex, start: int, end: int):
        self._recalculate_progress(start)
        index = self.index(0, 0, QModelIndex())
        self.dataChanged.emit(index, index, [ProgressRole])

    def _source_reset(self):
        self._recalculate_progress(0)
        self.modelReset.emit()
