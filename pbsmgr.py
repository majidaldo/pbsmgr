
##Author: Majid al-Dosari
#Copyright (c) 2010, Majid al-Dosari
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of the <organization> nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#Basic programmatic access to the PBS system (no external code reqd) and 
#linking of user PBS scripts on the filesystem to those on the PBS system 
#to allow for decision making.
#
#Main uses:
#- status monitoring
#- resubmission of scripts intended to continue
#from PBSQuery import PBSQuery
#pbsq=PBSQuery()
#pbsq.getjob('id') #pbsq.getjobs(attrlist)
#pbsmgt give me your batch id by nameid
#aj=p.getjob('12345.vmpsched')
#aj['Job_Name'] attrib.
#'job_state': ['R']
#aj.name for pid
#while much better, turns out i don't need it that much


import subprocess
import shlex
import os
import fnmatch
import time

"""
python thisfile.py --help
"""

def runc2(cmdline,stdoutt=subprocess.PIPE,stderrr=subprocess.PIPE):
    """does not block if error"""    
    #stdout=None for screen(?)"""
    args = shlex.split(cmdline)
    r=subprocess.Popen(args,stdout=stdoutt,stderr=stderrr)
    #r.terminate() #or kill()? #necessary? b/c it auto gets killed
    #r.stdout #fileobj
    return r.communicate()  #(stdo,stde)
def runc(*args,**kwargs):
    """blocks until no error"""
    #todo remove similar code from calls to this fnc
    OnE=runc2(*args,**kwargs)
    while OnE[1]!='': #meaning there's something wrong
        print 'ERROR in cmd:',args[0],' ',OnE[1]
        time.sleep(60)
        OnE=runc2(*args,**kwargs)
    return OnE#both outs. todo all calls shouls assume it will get teh desired
    #output b/c errors handled here
    

def namematcher(listofnames,listofpatterns):#intended for fnames, but what the heck
    """input pattern_S_"""
    matches=[]
    for apattern in listofpatterns:
        m= fnmatch.filter( listofnames , apattern)
        matches.extend(m)
    return matches

pbsattribstouse=['Job_Name','Job_Owner','job_state','ctime']
class pbsmgr(object): #objparams->taskid->pbsname,(pbsid on each submission)
    """
    REQUIREMENT!: unique pbsnames! i even check for it so you don't screw anything
    up.
    (Job_Name attrib set by #PBS -N somename must be unique!
    mapping: pbsjobname->a pbs file->pbsjobid
    why? it's the most flexible way b/c it's the only way to link w/ a particular 
    file in the file sys w/o going through some intermediate table
    
    methods w/ _* are unsafe
    methods with action_* perform execution actions
    other methods are just info look up, generally safe
    """
    def __init__(self,**kwargs):
        self.kwargs=kwargs
        sksd=self.kwargs.setdefault
        #sksd('inclpbsnames',[]) pointless
        sksd('pbspatterns',['*.pbs','pbs','*pbs','/pbs','/*pbs'])
        sksd('listofpathstopbsfiles',[])
        sksd('incldirs',[])
        sksd('excludepbsnamepatterns',[])#a list of them. cool idea:
            #mark 'XXX' in pbs script and include '*XXX*' in list
        sksd('excludedirs',[])
        sksd('qselectfilter','')#eg ' -u username'
        actions=dict.fromkeys(['keeprunning'],[])
        sksd('actions',actions)
        self.pbsidattribs=pbsattribstouse#only..
                #..these i found useful, Job_Name is a MUST
        self.pbsnameattribs=['pbsfile','jobid']
        self.pbsnameattribs.extend(self.pbsidattribs)
        
        return
    
    def getlistofpbsjobsinsys(self,qselectfilter='',blocking=True): 
        """via qselect + filter ops to get jobs on system.
        blocking=True only returns when it has the info"""
        if qselectfilter=='':qselectfilter=self.kwargs['qselectfilter']
        #below code inserted to deal w/ sys downs
        def qselecto(): return runc('qselect '+qselectfilter)
        qso=qselecto()
        while qso[1]!='': #meaning there's something wrong
            print qso[1]
            if blocking==True:#try again
                time.sleep(60)
                qso=qselecto()
            else: return
        return qso[0].split()
        #return runc('qselect '+qselectfilter)[0].split() 
    
    def getpbsnamesinjobs(self,listofpbsjobs):
        """the method to id a particular /script/ (not job). i have found that
        this is the most robust way to find a job in the pbs,
        but the user must supply a name in hiser script
        attrib str is 'Job_Name'
        job names must:
            -be less than 15chars
            -begin with alphabetic
            -no spaces
        note only first match will be taken. user must ensure unique job names
        """
        results=[]
        for apbsid in listofpbsjobs:
            results.append(self.getjobinfo(apbsid,['Job_Name'])[0])
        return results
        
    def getjobinfo(self,pbsid,attriblist):
        #pbsq.getjob(pbsid)[attrib] using the module is more robust
        #but the following will do for most purposes
        #returns {} if it doesn't exist
        statustxt=runc('qstat '+pbsid+' -f')[0]
        linesofstatus=statustxt.splitlines()
        attribvals=[]
        for anattrib in attriblist:
            for aline in linesofstatus:
                if anattrib in aline:# assumes line by line so 
                #attrib Variable_List wont parse
                #b/c they are on more than one line but i don't care.
                    attribvals.extend([aline.rpartition(' = ')[2]])
                    break
        return attribvals
        
    def getpbsnamesfromfiles(self,listoflocationstopbsfiles):
        names=[]
        for apbsfile in listoflocationstopbsfiles:
            aname=None
            pf=open(apbsfile)
            pbstxt=pf.read()
            pf.close() #lesson: close your files after use!
            pbsscript=pbstxt.splitlines()
            for aline in pbsscript:
                if '#PBS -N' in aline and '##' not in aline:#to acct for commented lines
                    aname=aline.rsplit('#PBS -N ')[1]
                    aname=aname.split()[0]
            names.append(aname)
        return names
        
    def getpbsfiles(self,dirr,followlinksv=False,**kwargs):#
        """keep in mind pbs file match pattern searches full directory path"""
        self.kwargs.update(kwargs)
        w=os.walk(dirr,followlinks=followlinksv)
        matches=[]
        for p,d,fns in w:
            fnp=[os.path.join(p,afn) for afn in fns]
            for apattern in self.kwargs['pbspatterns']:
                m= fnmatch.filter( fnp , apattern)
                matches.extend(m)
        return matches
    
    def chkuniquenamesinpbsfiles(self,listofpbsfiles):
        #i didn't see a need to chk pbsnames of pbsscripts already submitted
        namesinfiles=self.getpbsnamesfromfiles(listofpbsfiles)
        recurrentnames={}
        if len(frozenset(namesinfiles))!=len(listofpbsfiles) or None in namesinfiles:
            for apbsf in listofpbsfiles:
                for apbsn in namesinfiles:
                    if namesinfiles.count(apbsn)>1:
                        recurrentnames.update({apbsf:apbsn})
            return False,namesinfiles,recurrentnames
        else: return True,namesinfiles,recurrentnames
    
    def action_kill(self,pbsid):pass#list
    def action_hold(self,pbsid):pass#list
    def action_qalter(self,pbsid,args):pass
    
    def _qsub(self,scriptdir,qsubops=''):#todo: listsc
        """dangerous if same jobname already submitted AND running"""
        p=os.path.split(scriptdir)
        os.chdir(p[0])
        qo= runc('qsub '+scriptdir+qsubops)
        if qo[1]!='': print "qsub error", qo[1]
        return qo[0]
        #no problem if there is an error
        
    def _qdel(self,pbsid,qdelops=''):#todo: listsc
        so= runc('qdel '+pbsid+qdelops)
        return so[0],so[1]
        
#    def qsub(self,pbsjobname,qsubops=''):
#        #dont allow if pbsname in script in
#        #dont allow if pbsname in script is in sys AND running

#        on 2nd thought, i don't see a need if you submit something manually
#        it's already something new unless you're an idiot.
#        return
    
    def mappbsnames2files(self,**kwargs):
        """
        0. list all managed files excluding excluded dirs
        1. add pathstopbsfiles
        2. takeout excluded files
        4. check uniqueness of names
        5. take out excluded names
        5. map pbsnames to files
        """
        #if any argument provided, updates its defaults (and 'sticks' 
        #until next fnc call)
        self.kwargs.update(kwargs)
        manageddirs=frozenset(self.kwargs['incldirs'])\
            -frozenset(self.kwargs['excludedirs'])
#        md=[]
#        for adir in manageddirs:
#            for ap,ad,af in os.walk(adir):
#                md.append(ap)
#        md=frozenset(md)-frozenset(self.kwargs['excludedirs'])
        pbsfilestomanage=[]
        #print md,self.kwargs['excludedirs']
        for adir in manageddirs:
            pbsf=self.getpbsfiles(adir)
            pbsfilestomanage.extend(pbsf)
        #take out files from excluded dirs
        pbsfx=[]
        for axldir in self.kwargs['excludedirs']:
                pbsfx.extend(self.getpbsfiles(axldir))
        
        pbsfilestomanage=list(frozenset(pbsfilestomanage+self.kwargs['listofpathstopbsfiles'])\
        -frozenset(pbsfx))
        
        chk=self.chkuniquenamesinpbsfiles((pbsfilestomanage))
        if chk[0]==False:
            print chk[2]
            raise Exception, "pbs names in input pbs files not unique or at least one is unnamed"
        xclns=namematcher(chk[1],self.kwargs['excludepbsnamepatterns'])
        d=dict( zip(chk[1],pbsfilestomanage) )
        for an in xclns: d.pop(an)
        return d
        
    
    def genjobsinfotbl(self):#VIP!
        """
        MAIN INFO NEEDED FROM THIS METHOD! coordination comes from the output
        of this method
        gets specified jobs from pbs sys
        1. gets (optionally filtered) jobs from sys
        2. matches name to pbsjob id
        3. attribs
        """
        lpbsj=self.getlistofpbsjobsinsys()#jobs in sys
        jid2attribs={}
        for ajobid in lpbsj:
            ad=dict(zip(self.pbsidattribs,self.getjobinfo(ajobid,self.pbsidattribs)))
            jid2attribs.update({ajobid:ad})
        n2f=self.mappbsnames2files()
        #map w name k
        dbyname=dict.fromkeys(n2f.keys())
        for aname in dbyname.keys():
            pbsnad=dict.fromkeys(self.pbsnameattribs)
            pbsnad.pop('Job_Name')
            pbsnad.update({'pbsfile':n2f[aname]})
            dbyname.update({aname:pbsnad})
        namesinsys=[jid2attribs[ajob]['Job_Name'] for ajob in lpbsj]
        myjobnsinsys=frozenset.intersection(frozenset(n2f.keys()),frozenset(namesinsys))
        for ajn in myjobnsinsys:
            jids_highestnofirst=jid2attribs.keys()
            jids_highestnofirst.sort(reverse=True)
            for aji in jids_highestnofirst: #to only get latest* info for a jobname
                if jid2attribs[aji]['Job_Name']==ajn:
                    fji=aji#found LATEST* jobid corresponding to ajn
                    break #<-important!
            d=jid2attribs[fji].copy()
            d.pop('Job_Name')
            dbyname[ajn].update(d)
            dbyname[ajn]['jobid']=fji
        self.jobinfobyname=dbyname
        self.jobinfobypbsid=jid2attribs
        return dbyname,jid2attribs
    
    def action_runifnotrunning(self,listofpbsnames):
        """runs the assoc script if the job doesn't exist in the sys or if it's
        complete"""
        o=[]
        if len(listofpbsnames)==0:return o
        jobsinfo=self.genjobsinfotbl()[0]
        for an in listofpbsnames:
            if jobsinfo[an]['job_state']==None\
            or jobsinfo[an]['job_state']=='C':#b/c C is the *latest* status
                # so won't 'rerun' on job states Exiting, Hold, Running,
                #Suspend, Transitioning, Waiting..
                o.append(self._qsub(jobsinfo[an]['pbsfile']))
            else: o.append(None)
        return o
    
    
    def action_keeprunning(self,listofjobnames
        ,refreshtime=60*5,idiotproof=True):
        """
        input listofjobnames, timer in secs
        jobnames are those that passed the inclusion and exclusion process.
        
        for the any job in this list it is only safe if you manually submit a job before the
        script does. but strictly spking, you don't know if the script picked
        it up before you did. this is especially important if you input ALL
        managed pbs files..which you can do as yourobjinst.genjobsinfotbl().keys()
        so please let this method do its job. in any case, nothing will prevent you from
        manually submitting the same script twice from the cmd line.
        this script offers no protection from stupidity."""
        #if i'm working w/ the pbs system i can just loop w/in the pbs
        #sys with a script"""
        #PBS -N loopme
        # t= epoch time
        # qsub w/ Execution time delayed by (at least) t+walltime
        #qselect -N loopme > joblist
        #qdel < joblist   ...will this line work?
        self.kwargs['actions']['keeprunning']=listofjobnames #args[0]#update(kwargs)
        kr=self.kwargs['actions']['keeprunning']
        
        while True:
            if kr=='ALL': jns=self.genjobsinfotbl()[0].keys()
            else: jns=kr
            
            o=self.action_runifnotrunning(jns)
            
            #to attempt to idiotproof:
            #bad: both Q, 2nd one Q 1st R , both R
            if idiotproof==True:
                #loop thru pbsidinfo by ordered keys low to hi.
                #for each jid go 
                dbyid=self.jobinfobypbsid
                sortedids=dbyid.keys()
                sortedids.sort()
                deleted=[]
                for apbsid in sortedids:
                    if apbsid not in deleted:
                        statusofolderone=dbyid[apbsid]['job_state']
                        remainingids=sortedids[sortedids.index(apbsid)+1:]
                        for arid in remainingids:
                            if dbyid[arid]['Job_Name'] == dbyid[apbsid]['Job_Name']:
                                #statusofolderone = dbyid[arid]['job_state']
                                statusofnewerone = dbyid[arid]['job_state']
                                if ((statusofolderone  == 'Q') and ('Q'== statusofnewerone))\
                                or ((statusofolderone  == 'R') and ('Q'== statusofnewerone))\
                                or ((statusofolderone  == 'R') and ('R'== statusofnewerone)):
                                    qdo=self._qdel(arid)
                                    deleted.append(arid)
                                    if qdo[0] or qdo[1] != '': #ie something happened
                                        print 'qdel output: ', qdo[0], qdo[1]
                                    else: print 'deleted conflicting job', arid , 'for', dbyid[apbsid]['Job_Name']
        
            name_output=zip(jns,o)
            for aname,ano in name_output:
                if ano != None:
                    print "qsub output: ", ano.replace('\n',''), "for ", aname 
            time.sleep(refreshtime) #secs

        return #yeah right..        
                
#        self.kwargs['actions']['keeprunning']=listofjobnames #args[0]#update(kwargs)
#        kr=self.kwargs['actions']['keeprunning']
#        
#        while True:
#            if kr=='ALL': jns=self.genjobsinfotbl()[0].keys()
#            else: jns=kr
#            
#            o=self.action_runifnotrunning(jns)
#            
#            #to attempt to idiotproof:
#            #bad: both Q, 2nd one Q 1st R , both R
#            if idiotproof==True:
#                #loop thru pbsidinfo by ordered keys low to hi.
#                #for each jid go 
#                dbyid=self.jobinfobypbsid
#                sortedids=dbyid.keys()
#                sortedids.sort()
#                deleted=[]
#                for apbsid in sortedids:
#                    if apbsid not in deleted:
#                        statusofolderone=dbyid[apbsid]['job_state']
#                        remainingids=sortedids[sortedids.index(apbsid)+1:]
#                        for arid in remainingids:
#                            if dbyid[arid]['Job_Name'] == dbyid[apbsid]['Job_Name']:
#                                #statusofolderone = dbyid[arid]['job_state']
#                                statusofnewerone = dbyid[arid]['job_state']
#                                if ((statusofolderone  == 'Q') and ('Q'== statusofnewerone))\
#                                or ((statusofolderone  == 'R') and ('Q'== statusofnewerone))\
#                                or ((statusofolderone  == 'R') and ('R'== statusofnewerone)):
#                                    qdo=self._qdel(arid)
#                                    deleted.append(arid)
#                                    if qdo[0] or qdo[1] != '': #ie something happened
#                                        print 'qdel output: ', qdo[0], qdo[1]
#                                    else: print 'deleted conflicting job', arid , 'for', dbyid[apbsid]['Job_Name']
#            
#            
#            name_output=zip(jns,o)
#            for aname,ano in name_output:
#                if ano != None:
#                    print "qsub output: ", ano.replace('\n',''), "for ", aname 
#            time.sleep(refreshtime) #secs
#        
#
#        return #yeah right..
        #could spawn a proc but it should be mgd separately
        

def outputjobchanges(oldtable,newtable,attribstomon=['job_state','pbsid','Job_Name']):
    """returns empty lists if nothing changed"""
    addedjobs=frozenset(newtable.keys())-frozenset(oldtable.keys())
    deletedjobs=frozenset(oldtable.keys())-frozenset(newtable.keys())
    samejobs=frozenset.intersection(
        frozenset(newtable.keys()),frozenset(oldtable.keys())  )
    changedjobs={}
    for ajob in samejobs:
        changedattribs={}
        for anat in attribstomon:
            try: #if no attrib available
                if oldtable[ajob][anat]!=newtable[ajob][anat]:
                    changedattribs.update(  
                        {anat:(oldtable[ajob][anat],newtable[ajob][anat])}   )
                    changedjobs.update({ajob:changedattribs})
            except: continue
    return list(addedjobs), changedjobs, list(deletedjobs) #jobnames or ids



if __name__ == '__main__':
    #idiot-proof option?
    from optparse import OptionParser
    usage='Resubmits PBS scripts in current and below directories.\
    Conflicting submissions are removed.'
    parser = OptionParser(usage=usage)
    import getpass
    uid=getpass.getuser()
    parser.add_option("--userid",'-u', dest="userid",default=uid
              ,help="filter jobs in system by userid. default: "+uid)
    cd=os.getcwd()
    parser.add_option("--dir",'-d', dest="dirs",default=[cd]
                ,action='append'
              ,help="monitor jobs in dirs. you can specify more than one\
              dir by repeating the option. default is just cd: "+cd)
    parser.add_option("--refresh",'-r', dest="refresh",default=300
              ,help="refresh time. default: 300 secs",type=int)
    
    (options, args) = parser.parse_args()
    
    mainpbsmgr=pbsmgr(incldirs=options.dirs #AGAIN! i fail to input as a list!
    ,qselectfilter=' -u '+uid)
    print "WARNING: Be careful not to (re-)submit jobs picked up by this script!"
    print 'ctrl-c to break'
    mainpbsmgr.action_keeprunning('ALL',refreshtime=60)
    
    

    

