#ifndef MAINWINDOW_H
#define MAINWINDOW_H
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
 * \file mainwindow.h
 * \author Jharrod LaFon
 * \date Summer 2010
 */
#include <QDebug>
#include <QMainWindow>
#include <QFileSystemModel>
#include "pfilesystemmodel.h"
namespace Ui {
    class MainWindow;
}

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    explicit MainWindow(QWidget *parent = 0);

    ~MainWindow();
public slots:
    void fileLayoutChanged(QString);
    void debug(QString);
    void delButtonClicked();
    void setQueryString(QString);
    void search(QString);

protected:
    void changeEvent(QEvent *e);
//    void keyPressEvent(QKeyEvent *);



private:
    Ui::MainWindow *ui;
    PFileSystemModel *model;
    bool showAdvanced;
    QSettings settings;
  enum { MATCH, MATCHNOCASE, NOMATCH, NOMATCHNOCASE };
private slots:
    void on_pushButton_2_clicked();
    void on_tabWidget_currentChanged(int index);
    void on_pushButton_clicked();
    void on_lineEdit_returnPressed();
    void on_textBrowser_textChanged();
};

#endif // MAINWINDOW_H
