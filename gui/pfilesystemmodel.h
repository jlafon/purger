#ifndef PFILESYSTEMMODEL_H
#define PFILESYSTEMMODEL_H
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
 * \file pfilesystemmodel.h
 * \author Jharrod LaFon
 * \date Summer 2010
 * \brief A model to represent a parallel file system as a tree.
 */
#include "pfilenode.h"
#include <QtSql>
#include <QVariant>
#include <QAbstractItemModel>
#include <QSqlDriver>
/**
  * \class PFileSystemModel
  * \brief A derived class from QAbstractItemModel to model the parallel file system.
  * Uses a database to gather table data.
  */
class PFileSystemModel : public QAbstractItemModel
{
    Q_OBJECT
public:
    //! Constructor
    explicit PFileSystemModel(QObject *parent = 0);

    //! Overloaded data function
    /*!
      * \returns a QVariant containing the model data for the given index.
      */
    QVariant data(const QModelIndex &index, int role) const;

    //! Function to return the item flags
    /*!
      * \returns Flags pertaining to how the item is modeled.
      */
    Qt::ItemFlags flags(const QModelIndex &index) const;

    //! Function to get the headerData for the model.
    /*!
      * Returns the values for fields in the rootItem.
      */
    QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const;

    //! Function to return an index for the given parameters.
    QModelIndex index(int row, int column, const QModelIndex &parent = QModelIndex()) const;

    //! Returns the parent item for a given child.
    QModelIndex parent(const QModelIndex &child) const;

    //! Returns the best model matching the string
    QModelIndex search(const QString) const;

    //! Returns the list of fields for a given table
    QStringList getTableFields();

    //! Returns the row count of a given item.
    int rowCount(const QModelIndex &parent = QModelIndex()) const;

    //! Returns the column count of a given item.
    int columnCount(const QModelIndex &parent = QModelIndex()) const;

    //! Sorts the model based on the column given.
    /*!
      * \todo Make this work.
      */
    void sort(int column, Qt::SortOrder order);


    //! Destructor
    /*!
      * \todo Free all the memory
      */
    ~PFileSystemModel();
    void setQueryString(QString query);
    QList<QVariant> getDatabaseInfo();
    void setDatabaseInfo(QList<QVariant>);
    bool connectToDatabase();
    bool setupDatabase();

signals:
    //! For debugging
    /*!
      * \todo Remove this
      */
    void statusChanged(QString msg);
    void queryStringChanged(QString query);

public slots:
    void expandChild(QModelIndex parent);

protected:
    void fetchMore(const QModelIndex &parent);
    bool canFetchMore(const QModelIndex &parent) const;

private:
    void setupModelData(pFileNode* parent);
    pFileNode* rootItem;
    QSqlDatabase db;
    QSqlTableModel *tableModel;
};

#endif // PFILESYSTEMMODEL_H
