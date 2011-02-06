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
 * \file mainwindow.cpp
 * \author Jharrod LaFon
 * \date Summer 2010
 * \brief A window class to present parallel file system data.
 */

#include <QMessageBox>
#include <QPushButton>
#include <QKeyEvent>
#include "mainwindow.h"
#include "ui_mainwindow.h"


//! Constructor for the MainWindow
/**
  \param QWidget *parent
*/
MainWindow::MainWindow(QWidget *parent) :
    QMainWindow(parent),
    ui(new Ui::MainWindow)
{
    // Initialize UI
    ui->setupUi(this);

    // Hide the lower text box
    ui->lineEdit_2->hide();
    showAdvanced = true;

    // Show version
    ui->textBrowser->append("PathFinder 0.0.1");

    // Initialize new model
    model = new PFileSystemModel();

    // Set up connections
    connect(model, SIGNAL(statusChanged(QString)), this, SLOT(debug(QString)));
    connect(model, SIGNAL(queryStringChanged(QString)), this, SLOT(setQueryString(QString)));
    connect(model, SIGNAL(dataChanged(QModelIndex,QModelIndex)), ui->treeView, SLOT(dataChanged(QModelIndex,QModelIndex)));
    connect(ui->delButton, SIGNAL(clicked()), this, SLOT(delButtonClicked()));
    connect(ui->lineEdit_2, SIGNAL(textChanged(QString)), this, SLOT(search(QString)));

    // Connect to DataBase and Initialize query
    model->connectToDatabase();
    model->setupDatabase();

    // Associate the tree model to the view
    ui->treeView->setModel(model);

    // Set up filter stuff
    ui->fieldBox->insertItems(0,model->getTableFields());
    QStringList regexOptions;
    regexOptions << "match regex" \
                 << "match regex case sensitive" \
                 << "does not match regex" \
                 << "does not match regex case sensitive";
    ui->regexBox->insertItems(0,regexOptions);

    // Set up Icons and Table
    ui->pushButton->setIcon(QIcon::fromTheme("object-flip-vertical"));
    ui->delButton->setIcon(QIcon::fromTheme("edit-delete"));
    for(int i = 0; i < ui->treeView->header()->count(); i++)
        ui->treeView->header()->setResizeMode(i,QHeaderView::ResizeToContents);

    //Looks pretty but runs slow
    //ui->treeView->setAnimated(true);

    // Set up child expansion
    connect(ui->treeView, SIGNAL(expanded(QModelIndex)), model, SLOT(expandChild(QModelIndex)) );

    on_tabWidget_currentChanged(1);

    }
//! Function to change the query string displayed.
void MainWindow::setQueryString(QString msg)
{
    ui->lineEdit->setText(msg);
}

//! Function to handle file deletion.
/**
  * \todo Get this feature working
  */
void MainWindow::delButtonClicked()
{
    ui->textBrowser->append("<font color=red>Feature not implemented.</font>");
}

//! Function to display debugging messages
void MainWindow::debug(QString s)
{
    ui->textBrowser->append(s);
}

//! Function to send a message to the status bar
void MainWindow::fileLayoutChanged(QString s)
{
    ui->statusBar->showMessage(s);
}

//! Destructor
MainWindow::~MainWindow()
{
    delete ui;
    delete model;

}

//! Handle other languages
void MainWindow::changeEvent(QEvent *e)
{
    QMainWindow::changeEvent(e);
    switch (e->type()) {
    case QEvent::LanguageChange:
        ui->retranslateUi(this);
        break;
    default:
        break;
    }
}

//! Unimplemented
void MainWindow::on_textBrowser_textChanged()
{

}

//! This function is called when a user presses return after typing in a new search statement.
/** No validation is done */
void MainWindow::on_lineEdit_returnPressed()
{
    QString field = ui->fieldBox->currentText();
    QString regexOperator = QString("");
    int rtype = ui->regexBox->currentIndex();
    switch(rtype)
    {
    case MATCH: regexOperator = "~";
        break;
    case MATCHNOCASE: regexOperator = "~*";
        break;
    case NOMATCH: regexOperator = "!~";
        break;
    case NOMATCHNOCASE: regexOperator = "!~*";
        break;
    }
    QString query = QString("%0 %1 '%2'").arg(field).arg(regexOperator).arg(ui->lineEdit->text());
    ui->textBrowser->append(QString("Updating query filter to <font color=blue><em>%1</em></font>").arg(query));
    //ui->textBrowser->append(QString("Updating query filter to <font color=blue><em>%1</em></font>").arg(ui->lineEdit->text()));
    //filename ~ '^/+usr/+include/+[a-zA-Z0-9_-\s\.]+/+[a-zA-Z0-9_-\s\.]*$';
    model->setQueryString(query);

}

//! Function to search the gui for a term, not yet implemented.
/** \todo Implement the gui search */
void MainWindow::search(QString s)
{

    qDebug() << "Searching for " << s;
}

//! Function to switch between the basic and advanced controls
void MainWindow::on_pushButton_clicked()
{
    //! Check to see if all controls are to be shown.
    if(showAdvanced)
        {
        ui->pushButton->setText("Show");
        settings.setValue("splitterSizes", ui->splitter->saveState());
        QList<int> sizes = ui->splitter->sizes();
        sizes.replace(1,0);
        sizes.replace(2,0);
        sizes.replace(3,ui->pushButton->height());
        ui->splitter->setSizes(sizes);
        }
    else
        {
        ui->pushButton->setText("Hide");
        ui->splitter->refresh();
        ui->splitter->restoreState(settings.value("splitterSizes").toByteArray());
        }
    showAdvanced = !showAdvanced;
}

void MainWindow::on_tabWidget_currentChanged(int index)
{
    if(index == 0)
        return;
    QList<QVariant> info = model->getDatabaseInfo();
    ui->lineEdit_3->setText(info.at(0).toString());
    ui->lineEdit_4->setText(info.at(1).toString());
    ui->lineEdit_5->setText(info.at(2).toString());
    ui->lineEdit_6->setText(info.at(3).toString());

}

void MainWindow::on_pushButton_2_clicked()
{
    QList<QVariant> info;
    info << ui->lineEdit_3->text();
    info << ui->lineEdit_4->text();
    info << ui->lineEdit_5->text();
    info << ui->lineEdit_6->text();
    info << ui->lineEdit_7->text();
    model->setDatabaseInfo(info);

}
