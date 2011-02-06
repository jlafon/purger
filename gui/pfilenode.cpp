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
 * \file pfilenode.cpp
 * \author Jharrod LaFon
 * \date Summer 2010
 * \brief A class to represent items in a PFileSystemModel.
 */

#include "pfilenode.h"
#include "stdint.h"
#include <QStringList>
#include <QHash>
#include <QBitArray>
#include <QDebug>
pFileNode::pFileNode(const QList<QVariant> &data, pFileNode *parent)
{http://i.imgur.com/lWKUH.jpg
    isDir = false;
    parentItem = parent;
    itemData = data;
    QByteArray tmp = itemData.value(2).toByteArray();
    bool ok;
    uint64_t temp = tmp.toLong(&ok,2);
    if(temp & 0b1000000000)
        {
        setDir();
        }

}
void pFileNode::addData(QList<QVariant> &data)
{
    itemData.append(data);
}

void pFileNode::setData(QList<QVariant> &data)
{
    itemData.clear();
    itemData.append(data);
}
bool pFileNode::hasChildren() const
{
    return this->isDir;
}

pFileNode::~pFileNode()
{
    qDeleteAll(childItems);
}
void pFileNode::appendChild(pFileNode *item)
{
    setDir();
    childItems.append(item);
}
pFileNode* pFileNode::child(int row)
{
    return childItems.value(row);
}
int pFileNode::childCount() const
{
    return childItems.count();
}
int pFileNode::row() const
{
    if(parentItem)
        return parentItem->childItems.indexOf(const_cast<pFileNode*>(this));
    return 0;
}
int pFileNode::columnCount() const
{
    return itemData.count();
}
QVariant pFileNode::data(int column) const
{
    //This gets called a WHOLE lot
    if(parentItem && column == 2 && itemData.value(column).toInt() != 0)
        {
        QString mode;
        QByteArray tmp = itemData.value(column).toByteArray();
        bool ok;
        uint64_t temp = tmp.toLong(&ok,2);
        if(!ok)
            qDebug() << "<font color=red>Unable to convert file mode to decimal.</font>";


                 if(temp & 0b1)
                     mode.prepend("x");
                 else
                     mode.prepend("-");
                 if(temp & 0b10)
                     mode.prepend("w");
                 else
                     mode.prepend("-");
                 if(temp & 0b100)
                     mode.prepend("r");
                 else
                     mode.prepend("-");
                 if(temp & 0b1000)
                     mode.prepend("x");
                 else
                     mode.prepend("-");
                 if(temp & 0b10000)
                     mode.prepend("w");
                 else
                     mode.prepend("-");
                 if(temp & 0b100000)
                     mode.prepend("r");
                 else
                     mode.prepend("-");
                 if(temp & 0b1000000)
                     mode.prepend("x");
                 else
                     mode.prepend("-");
                 if(temp & 0b10000000)
                     mode.prepend("w");
                 else
                     mode.prepend("-");
                 if(temp & 0b100000000)
                     mode.prepend("r");
                 else
                     mode.prepend("-");
                 if(temp & 0b1000000000)
                     {
                     mode.prepend("d");
                     }
                 else
                     mode.prepend("-");
        return mode;

        }
    return itemData.value(column);
}
void pFileNode::removeChildren()
{
            childItems.clear();
}

pFileNode *pFileNode::parent()
{
    return parentItem;
}
