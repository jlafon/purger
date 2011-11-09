#!/opt/bin/python2.6
import ldap
import redis
import os
import sys
import base64

dbserver = "yell-fsdb"
ldapserver = "ldap://ldap.lanl.gov"
dn = "ou=people,dc=lanl,dc=gov"
unixdn = "ou=unixsrv,dc=lanl,dc=gov"

def getLdapEmail(uid):
    ldapHandle = ldap.initialize(ldapserver)
    ldapHandle.simple_bind_s()
    results = ldapHandle.search_s(dn,ldap.SCOPE_SUBTREE,"(uid="+uid+")",['mail'])
    print results    
    return results[0][1]['mail'][0]

def getLdapMoniker(uid):
#    print "Getting moniker for " + uid
    searchString = "(uidNumber="+uid+")"
    try:
        ldapHandle = ldap.initialize(ldapserver)
        ldapHandle.simple_bind_s()
        results = ldapHandle.search_s(unixdn,ldap.SCOPE_SUBTREE,searchString)
    except:
        print "LDAP Error" + str(e)
        return None
    print results
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
    print len
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
    files = getFileList(uid,dbno)
    try:
        outFile = open(name+"-expired-files.txt","w") 
    except Exception, e:
        print "Unable to create file for user " + name + ". Error: " + str(e)
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
            print file
            print filename
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
#             outFile.writelines(files)


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
