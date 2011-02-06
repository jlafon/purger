/*******************************************************************************
  Copyright 2008-2010 Los Alamos National Security, LLC. All Rights Reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

     o Redistributions of source code must retain the above copyright notice,
       this list of conditions and the following disclaimer.
     o Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in the
       documentation and/or other materials provided with the distribution.
     o Neither the name of the copyright holders nor the names of its
       contributors may be used to endorse or promote products derived from
       this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
  POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/

/**
 * \file pfilesystemmodel.cpp
 * \author Jharrod LaFon
 * \date Summer 2010
 * \brief A model to represent parallel file system data queried from a database.
 */
#include <stdio.h>
#include "pfilesystemmodel.h"
#include <QDebug>
#include "pfilenode.h"
#include <QHash>
#include <QInputDialog>
#include <QStack>

PFileSystemModel::PFileSystemModel(QObject *parent) :
    QAbstractItemModel(parent)
{

}
bool PFileSystemModel::setupDatabase()
{
    tableModel = new QSqlTableModel(this,db);
    tableModel->setTable("current_snapshot");
    tableModel->setSort(1, Qt::AscendingOrder);
    tableModel->select();

    // Get second column first entry which contains the name of the file table
    //QModelIndex myIndex = QAbstractItemModel::createIndex(0,1,0);
        //tableModel->setTable(tableModel->data(myIndex,0).toString());
    QStringList tables;
    for(int i = 0; i < tableModel->rowCount(); i++)
        tables.append(tableModel->record(i).value("name").toString());
    bool ok;
     QString i = QInputDialog::getItem(0,tr("Select a table"),tr("Available Tables"), tables,0,false,&ok);
    tableModel->setTable(i);
    emit statusChanged(QString("Loading table: ").append(tableModel->tableName()));
    tableModel->select();
    qDebug() << tableModel->query().lastError().text();

    QList<QVariant> rootData;
    rootData << "filename";
    rootData << "inode";
    rootData << "mode";
    rootData << "nlink";
    rootData << "uid";
    rootData << "gid";
    rootData << "size";
    rootData << "block";
    rootData << "block_size";
    rootData << "atime";
    rootData << "mtime";
    rootData << "ctime";
    rootData << "abslink";
    rootData << "added";
    rootItem = new pFileNode(rootData);
    emit statusChanged(QString("<font color=grey>Debug: Query size: %1</font>").arg(tableModel->query().size()));
    this->setupModelData(rootItem);
    return ok;
}
void PFileSystemModel::setQueryString(QString query)
{
    tableModel->setFilter(query);
    tableModel->select();
    rootItem->removeChildren();
    setupModelData(rootItem);
    emit statusChanged(QString("<font color=blue> %1</font>").arg(tableModel->query().lastQuery()));
    emit statusChanged(QString("<font color=red> %1</font>").arg(tableModel->query().lastError().text()));
}

void PFileSystemModel::sort(int column, Qt::SortOrder order)
{
    qDebug() << "Sorting column" << column;

}

QStringList PFileSystemModel::getTableFields()
{
    QStringList rootData;
    rootData << "filename"
    << "inode" \
    << "mode" \
    << "nlink"\
    << "uid" \
    << "gid" \
    << "size" \
    << "block" \
    << "block_size" \
    << "atime" \
    << "mtime" \
    << "ctime" \
    << "abslink" \
    << "added";
    return rootData;
}

void PFileSystemModel::setupModelData(pFileNode *parent)
{
    if(tableModel->rowCount() > 16584)
        emit statusChanged(QString("<font color=blue>More than 16k records returned.  Please apply a filter."));
    //Abandon all hope, ye who enter here
    QHash<QString, pFileNode* > ch;
    pFileNode* last = 0;

    //Create default item info
    QList<QVariant> defaultDat;
    for(int j = 0; j < 13; j++)
        defaultDat << " ";
    QStringList ls;
    //Iterate through all the records
    emit statusChanged(QString("<font color=grey>Debug: Rowcount = %1").arg(tableModel->rowCount()));
    for(int i = 0; i < tableModel->rowCount(); ++i)
            {
            //Create item data for each record
            QList<QVariant> itemData;
            //break apart the absolute file name
            ls = tableModel->record(i).value("filename").toString().split("/", QString::SkipEmptyParts);
            //qDebug() << "Item: " << ls.join("/");
            //get filename (without path)
            itemData << ls.last();
            //Insert fields
            for(int j = 1; j < 14; ++j)
                itemData << tableModel->record(i).value(j);
            //get path only
            ls = ls.mid(0,ls.length()-1);
            //get full name
            QString fullName = tableModel->record(i).value("filename").toString();
            //get depth of tree
            int length = ls.length();
            QString key;
            //set up directories
            for(int i = 0; i <= length; i++)
                {
                key = ((QStringList)(ls.mid(0,i))).join("/");
                if(i == 0 && !ch.contains(key))
                    {
                    QList<QVariant> dat;
                    if(key.split("/").last() != "")
                        dat << key.split("/").last();
                    else dat << QString("/");
                    dat.append(defaultDat);
                    last = new pFileNode(dat,rootItem);
                    ch.insert(key,last);
                    parent->appendChild(last);
                    }
                else if(i > 0 && !ch.contains(key))
                    {
                    QList<QVariant> dat;
                    dat << key.split("/").last();
                    dat.append(defaultDat);
                    QString s = ((QStringList)(ls.mid(0,i-1))).join("/");
                    pFileNode* p = ch.value(s);
                    last = new pFileNode(dat,p);
                    p->appendChild(last);
                    ch.insert(key,last);
                    }
                else
                    {
                    }

                }
           // qDebug() << "[2] Creating node " << itemData.first().toString();
            pFileNode* child = new pFileNode(itemData,last);
            ch.insert(fullName,child);
            if(last)
            last->appendChild(child);

    }
emit layoutChanged();
} //End functioni

QModelIndex PFileSystemModel::index(int row, int column, const QModelIndex &parent) const
{
    if(!hasIndex(row,column,parent))
        return QModelIndex();
    pFileNode* parentItem;
    if(!parent.isValid())
        parentItem = rootItem;
    else
        parentItem = static_cast<pFileNode*>(parent.internalPointer());

    pFileNode *childItem = parentItem->child(row);
    if(childItem)
        return createIndex(row,column,childItem);
    else
        return QModelIndex();
}

QModelIndex PFileSystemModel::parent(const QModelIndex &index) const
{
    if(!index.isValid())
        return QModelIndex();
    pFileNode *childItem = static_cast<pFileNode*>(index.internalPointer());
    pFileNode *parentItem = childItem->parent();

    if(parentItem == rootItem)
        return QModelIndex();

    return createIndex(parentItem->row(), 0, parentItem);
}
QModelIndex PFileSystemModel::search(const QString pattern) const
{
//    if(pattern.length() == 0 || rootItem->childCount() == 0)
        return QModelIndex();

}

int PFileSystemModel::rowCount(const QModelIndex &parent) const
{
    pFileNode *parentItem;
    if(parent.column() > 0)
        return 0;
    if(!parent.isValid())
        parentItem = rootItem;
    else
        parentItem = static_cast<pFileNode*>(parent.internalPointer());
    return parentItem->childCount();

}

int PFileSystemModel::columnCount(const QModelIndex &parent) const
{
    if(parent.isValid())
        return static_cast<pFileNode*>(parent.internalPointer())->columnCount();
    else
        return rootItem->columnCount();
}
QVariant PFileSystemModel::data(const QModelIndex &index, int role) const
{
    if(!index.isValid())
        return QVariant();

    if(role == Qt::DecorationRole && index.column() == 0)
        {
        pFileNode* item = static_cast<pFileNode*>(index.internalPointer());
        if(!item->hasChildren())
            return QIcon::fromTheme("text-x-generic");
        else
            return QIcon::fromTheme("folder");
        }
    if(role != Qt::DisplayRole)
        return QVariant();

    pFileNode* item = static_cast<pFileNode*>(index.internalPointer());
    return item->data(index.column());
}
void PFileSystemModel::expandChild(QModelIndex parent)
{
    if(!parent.isValid())
        return;
    pFileNode* item = static_cast<pFileNode*>(parent.internalPointer());
    for(int i = 0; i < item->childCount(); i++)
        if(item->child(i)->hasChildren())
            {
            qDebug() << "filename ~ " << item->child(i)->data(0).toString();
            emit layoutAboutToBeChanged();
            tableModel->setFilter(QString("filename ~ %1").arg(item->child(i)->data(0).toString()));
            tableModel->select();
            setupModelData(item->child(i));
            emit layoutChanged();
            }
    return;
}

void PFileSystemModel::fetchMore(const QModelIndex &parent)
{
//    beginInsertRows(QModelIndex(),0,0);
//    endInsertRows();
    //qDebug() << "PFileSystemModel::fetchMore";
    return;
}
bool PFileSystemModel::canFetchMore(const QModelIndex &parent) const
{
    return false;
    qDebug() << "PFileSystemModel::fetchMore";
    pFileNode* item = static_cast<pFileNode*>(parent.internalPointer());
    return item->hasChildren();
}
QList<QVariant> PFileSystemModel::getDatabaseInfo()
{
    QList<QVariant> info;
    info << db.hostName();
    info << db.port();
    info << db.databaseName();
    info << db.userName();
    return info;
}
void PFileSystemModel::setDatabaseInfo(QList<QVariant> info)
{
    emit statusChanged("Closing database connection.");
    db.close();
    db.setHostName(info.at(0).toString());
    db.setPort(info.at(1).toInt());
    db.setDatabaseName(info.at(2).toString());
    db.setUserName(info.at(3).toString());
    db.setPassword(info.at(4).toString());
    emit statusChanged("Connecting to database.");
    bool ok = db.open();
    if(ok)
        {
        emit statusChanged("<font color=blue>Connected to database.</font>");
        }
    else
        {
        emit statusChanged(QString("<font color=red>Unable to connect to database: %1</font>").arg(db.lastError().text()));
        }
    emit layoutAboutToBeChanged();
    setupDatabase();
    emit layoutChanged();
}

Qt::ItemFlags PFileSystemModel::flags(const QModelIndex &index) const
{
    if(!index.isValid())
        return 0;
    return Qt::ItemIsEnabled | Qt::ItemIsSelectable | Qt::ItemIsEditable;

}
QVariant PFileSystemModel::headerData(int section, Qt::Orientation orientation, int role) const
{
    if(orientation == Qt::Horizontal && role == Qt::DisplayRole)
        return rootItem->data(section);
    return QVariant();
}
bool PFileSystemModel::connectToDatabase()
{
    db = QSqlDatabase::addDatabase("QPSQL");
    db.setHostName("localhost");
    db.setDatabaseName("scratch");
    db.setPort(5432);
    db.setUserName("treewalk");
    db.setPassword("testing");

    bool ok = db.open();
    if(ok)
        {
        emit statusChanged("<font color=blue>Connected to database.</font>");
        }
    else
        {
        emit statusChanged(QString("<font color=red>Unable to connect to database: %1</font>").arg(db.lastError().text()));
        }

    emit statusChanged(QString("<font color=grey>Debug: DB Driver has multiple result sets: ").append(QString("%1").arg(db.driver()->hasFeature(QSqlDriver::MultipleResultSets))).append("</font>"));

    return ok;
}


PFileSystemModel::~PFileSystemModel()
{
    db.close();
    delete rootItem;
}

