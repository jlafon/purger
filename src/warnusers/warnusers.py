#!/opt/bin/python2.6
import ldap
import redis
import os
import sys
import base64
import smtplib
from email.mime.text import MIMEText
messagefile = "message.txt"
dbserver = "yell-fsdb"
ldapserver = "ldap://ldap.lanl.gov"
rootaddr = 'root@ydms-master.lanl.gov'
dn = "ou=people,dc=lanl,dc=gov"
unixdn = "ou=unixsrv,dc=lanl,dc=gov"
warnmessage = """
The following text file in the Yellow network contains a list of your scratch files that have not been modified in the last 14+ days.
Those files will be deleted in at least 6 days if not modified by then.  This notification may not have up-to-the-minute information,
but we will verify a file's actual age before purging it.  For more information, please see our purge policy:  
http://hpc.lanl.gov/purge_policy.  If you have any questions or concerns, please contact ICN Consultants at 505-665-444 option 3
or consult@lanl.gov."""

def getLdapEmail(uid):
    try:
        ldapHandle = ldap.initialize(ldapserver)
        ldapHandle.simple_bind_s()
        results = ldapHandle.search_s(dn,ldap.SCOPE_SUBTREE,"(uid="+uid+")",['mail'])
    except Exception, e:
        print "Error getting LDAP email address: " + str(e)
        raise
        return None
    if len(results) > 0:
       return results[0][1]['mail'][0]
    else:
       return None

def getLdapMoniker(uid):
    searchString = "(uidNumber="+uid+")"
    try:
        ldapHandle = ldap.initialize(ldapserver)
        ldapHandle.simple_bind_s()
        results = ldapHandle.search_s(unixdn,ldap.SCOPE_SUBTREE,searchString)
    except:
        print "LDAP Error" + str(e)
        return None
    if len(results) > 0:
         return results[0][1]['uid'][0]
    else:
         return None

def getUidList(dbno):
    try:
        con = redis.StrictRedis(host='yell-fsdb',port=6379,db=dbno)
        card = con.scard('warnlist')
    except Exception, e:
        print "Unable to connect to redis or scard failed:" + str(e)
        raise
    list = []
    for uid in range(card):
         list.append(con.spop('warnlist'))
    return list

def getFileList(uid,dbno):
    try:
        con = redis.StrictRedis(host='yell-fsdb',port=6379,db=dbno)
        len = con.scard(uid)
    except Exception, e:
        print "Unable to connect to redis server or scard failed:" + str(e)
        raise
    files = []
    try:
        files = con.smembers(uid)
    except Exception, e:
        print "Unable to get expired files for user:" + str(e)
        raise
    return files

def processUid(uid,dbno,scratch):
    name = getLdapMoniker(uid)
    if not name:
        print "Unable to get moniker for uid " + str(uid)
        return
    print name
    email = getLdapEmail(name)
    if not email:
        print "Unable to get email for uid " + str(uid)
        return
    files = getFileList(uid,dbno)
    try:
        outFile = open(name+"-expired-files.txt","w")
        msg = MIMEText(warnmessage + "\n/" + scratch + "/" + name + "/expired-files.txt")
        msg['Subject'] = '[Purger-Notification] ' + email
        msg['From'] = 'consult@lanl.gov'
        msg['To'] = 'jlafon@lanl.gov'
    except Exception, e:
        print "Unable to create file for user or unable to open message.txt" + name + ". Error: " + str(e)
        raise
    try:
        con = redis.StrictRedis(host='yell-fsdb',port=6379,db=dbno)
    except Exception, e:
        print "Redis Error: " + str(e)
        raise
    for file in files:
        try:
            filerecord = con.hmget(name=file,keys=['name'])
            filename = filerecord[0]
            # One line magic to make the string length a multiple of 3
            padlength = (3-(len(filename)%3))%3
            for i in range(padlength+1):
                filename = filename + '='
            decodedName = base64.b64decode(filename)
            outFile.write(decodedName + '\n')
        except Exception, e:
            print "Unable to connect to redis server or scard failed:" + str(e)
            raise
    outFile.close()
    try:
        s = smtplib.SMTP('mail.lanl.gov')
        s.sendmail(rootaddr,['jlafon@lanl.gov'],msg.as_string())
        s.quit()
        print "Sent email message"
    except Exception, e:
        print "Unable to send mail to " + email + ". Error: " + str(e)
        return
#    for file in files:
#       try:
            
     


def processUids(uids,dbno,scratch):
    for uid in uids:
        print "Processing " + uid
        processUid(uid,dbno,scratch)

def main():
    # print getLdapEmail('jlafon')
    dbno = 0
    uids = getUidList(dbno)
    print "Processing %d uids" % len(uids)
    processUids(uids,dbno,'scratch2')
    dbno = 1
    uids = getUidList(dbno)
    print "Processing %d uids" % len(uids)
    processUids(uids,dbno,'scratch3')

if __name__ == '__main__':
    main()
